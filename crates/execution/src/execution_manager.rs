use crossbeam_channel::{Receiver, Sender};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use unified_trading_core::clock::{Clock, WallClock};
use unified_trading_core::kill_switch::KillSwitch;
use unified_trading_core::metrics::GlobalMetrics;
use unified_trading_core::journal::{JournalHandle, JournalEntry};
use unified_trading_core::validator::RequestValidator;
use unified_trading_core::idempotency::IdempotencyStore;
use unified_trading_core::portfolio_manager::PortfolioManager;
use unified_trading_core::symbol_registry::next_request_id;
use unified_trading_core::threading::{spawn_pinned, ThreadPriority};

use crate::order_tracker::{OrderTracker, OrderStatus};
use crate::rate_limiter::RateLimiter;
use crate::order_lifecycle::{OrderLifecycleEvent, OrderLifecycleEventType};
use gateway::{BrokerError, CircuitBreaker, IExecutionPort, OrderCommand, OrderSide, OrderType, TimeInForce};
use risk::RiskDecision;

pub struct ExecutionManager {
    pub decision_rx: Receiver<RiskDecision>,
    pub lifecycle_tx: Sender<OrderLifecycleEvent>,
    pub execution_port: Arc<dyn IExecutionPort>,
    pub order_tracker: Arc<parking_lot::Mutex<OrderTracker>>,
    pub rate_limiter: Arc<parking_lot::Mutex<RateLimiter>>,
    pub circuit_breaker: Arc<CircuitBreaker>,
    pub idempotency_store: Arc<IdempotencyStore>,
    pub portfolio_manager: Arc<PortfolioManager>,
    pub metrics: Arc<GlobalMetrics>,
    pub journal_handle: Option<JournalHandle>,
    pub kill_switch: Arc<KillSwitch>,
    pub validator: RequestValidator,
    running: Arc<AtomicBool>,
    clock: Arc<dyn Clock>,
    max_retries: u32,
    retry_backoff_ms: u64,
}

impl ExecutionManager {
    pub fn new(
        decision_rx: Receiver<RiskDecision>,
        lifecycle_tx: Sender<OrderLifecycleEvent>,
        execution_port: Arc<dyn IExecutionPort>,
        global_rate: f64,
        per_symbol_rate: f64,
        metrics: Arc<GlobalMetrics>,
        kill_switch: Arc<KillSwitch>,
        portfolio_manager: Arc<PortfolioManager>,
        order_tracker: Arc<parking_lot::Mutex<OrderTracker>>,
        rate_limiter: Arc<parking_lot::Mutex<RateLimiter>>,
        circuit_breaker: Arc<CircuitBreaker>,
        idempotency_store: Arc<IdempotencyStore>,
        validator: RequestValidator,
        journal_handle: Option<JournalHandle>,
        max_retries: u32,
        retry_backoff_ms: u64,
    ) -> Self {
        Self::with_clock(
            decision_rx,
            lifecycle_tx,
            execution_port,
            global_rate,
            per_symbol_rate,
            metrics,
            kill_switch,
            portfolio_manager,
            order_tracker,
            rate_limiter,
            circuit_breaker,
            idempotency_store,
            validator,
            Arc::new(WallClock::new()),
            journal_handle,
            max_retries,
            retry_backoff_ms,
        )
    }

    pub fn with_clock(
        decision_rx: Receiver<RiskDecision>,
        lifecycle_tx: Sender<OrderLifecycleEvent>,
        execution_port: Arc<dyn IExecutionPort>,
        _global_rate: f64,
        _per_symbol_rate: f64,
        metrics: Arc<GlobalMetrics>,
        kill_switch: Arc<KillSwitch>,
        portfolio_manager: Arc<PortfolioManager>,
        order_tracker: Arc<parking_lot::Mutex<OrderTracker>>,
        rate_limiter: Arc<parking_lot::Mutex<RateLimiter>>,
        circuit_breaker: Arc<CircuitBreaker>,
        idempotency_store: Arc<IdempotencyStore>,
        validator: RequestValidator,
        clock: Arc<dyn Clock>,
        journal_handle: Option<JournalHandle>,
        max_retries: u32,
        retry_backoff_ms: u64,
    ) -> Self {
        Self {
            decision_rx,
            lifecycle_tx,
            execution_port,
            order_tracker,
            rate_limiter,
            circuit_breaker,
            idempotency_store,
            portfolio_manager,
            metrics,
            journal_handle,
            kill_switch,
            validator,
            running: Arc::new(AtomicBool::new(true)),
            clock,
            max_retries,
            retry_backoff_ms,
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn run_loop(&mut self) {
        while self.running.load(Ordering::Relaxed) && !self.kill_switch.is_active() {
            match self.decision_rx.recv_timeout(std::time::Duration::from_millis(10)) {
                Ok(decision) => {
                    if !decision.approved {
                        continue;
                    }

                    if self.kill_switch.is_active() {
                        tracing::warn!("Kill switch active, rejecting order");
                        continue;
                    }

                    if !self.circuit_breaker.can_execute() {
                        tracing::warn!("Circuit breaker open, rejecting order");
                        self.metrics.circuit_breaker_trips.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }

                    self.execute_decision(&decision);
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }
        }

        // Drain remaining decisions so they are not lost in the queue
        while let Ok(decision) = self.decision_rx.try_recv() {
            if decision.approved {
                tracing::warn!(request_id = %decision.request_id, "Draining unexecuted approved decision during shutdown");
            }
        }
    }

    /// Returns true if the error is transient and retryable.
    fn is_transient_error(e: &BrokerError) -> bool {
        matches!(e, BrokerError::ConnectionFailed(_) | BrokerError::RateLimited | BrokerError::Unknown(_))
    }

    /// Submit an order with exponential backoff retry for transient errors.
    fn submit_with_retry(
        &self,
        cmd: &OrderCommand,
        symbol_key: &str,
        intent_id: u64,
        decision: &RiskDecision,
        now: u64,
    ) -> Result<String, BrokerError> {
        let max_retries = self.max_retries;
        let base_delay_ms = self.retry_backoff_ms;

        for attempt in 0..max_retries {
            let result = self.execution_port.submit_order(cmd);

            match &result {
                Ok(_) => return result,
                Err(e) if Self::is_transient_error(e) => {
                    if attempt + 1 < max_retries {
                        let delay_ms = base_delay_ms * (1u64 << attempt); // 100, 200, 400
                        tracing::warn!(
                            symbol = %symbol_key,
                            intent_id = intent_id,
                            attempt = attempt + 1,
                            max_retries = max_retries,
                            delay_ms = delay_ms,
                            error = %e,
                            "Order submission transient error, retrying"
                        );
                        std::thread::sleep(std::time::Duration::from_millis(delay_ms));
                    } else {
                        tracing::error!(
                            symbol = %symbol_key,
                            intent_id = intent_id,
                            error = %e,
                            "Order submission failed after {} retries",
                            max_retries
                        );
                        return result;
                    }
                }
                Err(_) => {
                    // Non-transient error — fail immediately
                    return result;
                }
            }
        }

        // Shouldn't reach here, but satisfy the compiler
        self.execution_port.submit_order(cmd)
    }

    #[tracing::instrument(skip_all, fields(request_id = %decision.request_id))]
    fn execute_decision(&mut self, decision: &RiskDecision) {
        let now = self.clock.now_ns();

        let symbol_id = decision.request.symbol_id;
        let symbol_key = symbol_id.as_u16().to_string();

        if let Err(e) = self.validator.validate_symbol(&symbol_key) {
            tracing::warn!("Validation failed for symbol_id {:?}: {}", symbol_id, e);
            self.metrics.orders_rejected.fetch_add(1, Ordering::Relaxed);
            return;
        }

        {
            let mut rate_limiter = self.rate_limiter.lock();
            if !rate_limiter.try_consume(&symbol_key, 1.0) {
                self.metrics.orders_rejected.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(symbol_id = ?symbol_id, "Rate limit exceeded");
                return;
            }
        }

        let intent_id = decision.request.intent_id;
        let idempotency_key = format!("intent-{}", intent_id);
        if self.idempotency_store.is_processed(&idempotency_key) {
            tracing::warn!(idempotency_key = %idempotency_key, "Duplicate order detected");
            return;
        }

        let order_id = next_request_id().to_string();

        let side = match decision.request.side {
            0 | 2 | 4 => OrderSide::Buy,
            1 | 3 | 5 => OrderSide::Sell,
            other => {
                tracing::warn!("Unexpected side '{}', defaulting to Sell", other);
                OrderSide::Sell
            }
        };

        let quantity = decision.request.quantity;

        // Write Pending journal entry BEFORE submitting to broker
        // On crash recovery, all Pending entries are replayed/retried.
        if let Some(ref handle) = self.journal_handle {
            let _ = handle.try_write(JournalEntry::Order {
                symbol: symbol_key.clone(),
                timestamp_ns: now,
                data: format!(
                    "intent_id={},order_id={},side={:?},qty={},request_id={},status=Pending",
                    intent_id, order_id, side, quantity, decision.request_id
                ),
            });
        }

        let cmd = OrderCommand {
            order_id: order_id.clone(),
            symbol: symbol_key.clone(),
            side: side.clone(),
            quantity,
            order_type: OrderType::Market,
            limit_price: None,
            stop_price: None,
            time_in_force: TimeInForce::Day,
            correlation_id: decision.request_id.to_string(),
            trace_id: decision.trace_id,
        };

        let broker_start = self.clock.now_monotonic_ns();

        match self.submit_with_retry(&cmd, &symbol_key, intent_id, decision, now) {
            Ok(execution_id) => {
                let broker_end = self.clock.now_monotonic_ns();
                let rtt_ns = broker_end.saturating_sub(broker_start);
                self.metrics.broker_round_trip_latency.record(rtt_ns);
                self.metrics.broker_send_latency.record(rtt_ns);

                self.metrics.orders_submitted.fetch_add(1, Ordering::Relaxed);
                self.kill_switch.track_open_order(&execution_id);

                let lifecycle_event = OrderLifecycleEvent::new(
                    execution_id.clone(),
                    symbol_key.clone(),
                    OrderLifecycleEventType::Submitted,
                    now,
                )
                .with_status("submitted".to_string())
                .with_trace_id(decision.trace_id);

                self.idempotency_store.mark_processed(idempotency_key, execution_id.clone());

                // Mark as Submitted in journal (allows recovery to skip this intent)
                if let Some(ref handle) = self.journal_handle {
                    let _ = handle.try_write(JournalEntry::Order {
                        symbol: symbol_key.clone(),
                        timestamp_ns: now,
                        data: format!(
                            "intent_id={},order_id={},side={:?},qty={},request_id={},status=Submitted,execution_id={}",
                            intent_id, order_id, side, quantity, decision.request_id, execution_id
                        ),
                    });
                }

                self.metrics.lifecycle_channel_depth.fetch_add(1, Ordering::Relaxed);
                let _ = self.lifecycle_tx.send(lifecycle_event);

                tracing::info!(order_id = %execution_id, symbol = %symbol_key, "Order submitted to broker");

                self.circuit_breaker.record_success();
            }
            Err(e) => {
                self.metrics.orders_rejected.fetch_add(1, Ordering::Relaxed);
                self.circuit_breaker.record_failure();
                tracing::warn!(symbol = %symbol_key, error = %e, "Order submission failed");

                if let Some(ref handle) = self.journal_handle {
                    let _ = handle.try_write(JournalEntry::Event {
                        event_type: "ORDER_FAILED".to_string(),
                        timestamp_ns: now,
                        data: format!(
                            "intent_id={},symbol={},error={},status=Failed",
                            intent_id, symbol_key, e
                        ),
                    });
                }
            }
        }
    }

    pub fn on_fill(&mut self, order_id: &str, symbol: &str, filled_qty: f64, fill_price: f64, trace_id: u64) {
        let now = self.clock.now_ns();

        {
            let mut tracker = self.order_tracker.lock();
            tracker.update_fill(order_id, filled_qty);
        }
        self.kill_switch.remove_open_order(order_id);
        self.metrics.orders_filled.fetch_add(1, Ordering::Relaxed);
        self.circuit_breaker.record_success();

        self.portfolio_manager.on_fill(symbol, fill_price, filled_qty, true);

        let lifecycle_event = OrderLifecycleEvent::new(
            order_id.to_string(),
            symbol.to_string(),
            OrderLifecycleEventType::Filled,
            now,
        )
        .with_fill(filled_qty, fill_price)
        .with_status("filled".to_string())
        .with_trace_id(trace_id);

        let _ = self.lifecycle_tx.send(lifecycle_event);

        tracing::info!(order_id = %order_id, symbol = %symbol, qty = filled_qty, price = fill_price, trace_id = trace_id, "Order filled");
    }

    pub fn on_reject(&mut self, order_id: &str, symbol: &str, reason: &str, trace_id: u64) {
        let now = self.clock.now_ns();

        self.kill_switch.remove_open_order(order_id);
        self.metrics.orders_rejected.fetch_add(1, Ordering::Relaxed);
        self.circuit_breaker.record_failure();

        let lifecycle_event = OrderLifecycleEvent::new(
            order_id.to_string(),
            symbol.to_string(),
            OrderLifecycleEventType::Rejected,
            now,
        )
        .with_status(format!("rejected: {}", reason))
        .with_trace_id(trace_id);

        let _ = self.lifecycle_tx.send(lifecycle_event);
    }

    pub fn on_cancel(&mut self, order_id: &str, symbol: &str, trace_id: u64) {
        let now = self.clock.now_ns();

        {
            let mut tracker = self.order_tracker.lock();
            tracker.update_status(order_id, OrderStatus::Cancelled);
        }
        self.kill_switch.remove_open_order(order_id);
        self.metrics.orders_cancelled.fetch_add(1, Ordering::Relaxed);

        let lifecycle_event = OrderLifecycleEvent::new(
            order_id.to_string(),
            symbol.to_string(),
            OrderLifecycleEventType::Cancelled,
            now,
        )
        .with_status("cancelled".to_string())
        .with_trace_id(trace_id);

        let _ = self.lifecycle_tx.send(lifecycle_event);
    }

    pub fn start(mut self, core_id: usize) -> std::thread::JoinHandle<()> {
        spawn_pinned(
            "execution-manager",
            core_id,
            ThreadPriority::High,
            move || {
                self.run_loop();
            },
        ).expect("spawn_pinned failed")
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::bounded;
    use unified_trading_core::clock::wall_time_ns;
    use gateway::MockExecutionPort;

use unified_trading_core::symbol_registry::SymbolId;

    fn make_approved_decision() -> RiskDecision {
        let now = wall_time_ns();
        let request = risk::RiskCheckRequest {
            request_id: unified_trading_core::symbol_registry::next_request_id(),
            symbol_id: SymbolId::from_raw(0),
            intent_id: unified_trading_core::symbol_registry::derive_intent_id(1),
            side: 1u8, // Buy
            quantity: 10.0,
            price: 150.0,
            timestamp_ns: now,
            current_volatility: 0.01,
            current_spread_bps: 10.0,
            trace_id: 1,
        };
        RiskDecision {
            request_id: request.request_id,
            approved: true,
            rejection_reason: None,
            warnings: Vec::new(),
            check_index: 14,
            timestamp_ns: now,
            request,
            trace_id: 1,
        }
    }

    fn make_manager(
        dec_rx: Receiver<RiskDecision>,
        lifecycle_tx: Sender<OrderLifecycleEvent>,
        global_rate: f64,
        per_symbol_rate: f64,
        metrics: Arc<GlobalMetrics>,
        kill_switch: Arc<KillSwitch>,
        portfolio_manager: Arc<PortfolioManager>,
        execution_port: Arc<dyn IExecutionPort>,
    ) -> ExecutionManager {
        let order_tracker = Arc::new(parking_lot::Mutex::new(OrderTracker::new()));
        let rate_limiter = Arc::new(parking_lot::Mutex::new(RateLimiter::new(global_rate, per_symbol_rate)));
        let circuit_breaker = Arc::new(CircuitBreaker::new(5, 30_000));
        let idempotency_store = Arc::new(IdempotencyStore::new());
        ExecutionManager::new(
            dec_rx,
            lifecycle_tx,
            execution_port,
            global_rate,
            per_symbol_rate,
            metrics,
            kill_switch,
            portfolio_manager,
            order_tracker,
            rate_limiter,
            circuit_breaker,
            idempotency_store,
            RequestValidator::default(),
            None,
            3,
            100,
        )
    }

    #[test]
    fn test_execution_manager_processes_approved() {
        let (dec_tx, dec_rx) = bounded::<RiskDecision>(100);
        let (lifecycle_tx, lifecycle_rx) = bounded::<OrderLifecycleEvent>(100);
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        let portfolio_manager = Arc::new(PortfolioManager::new(100_000.0, 0.001));

        let manager = make_manager(
            dec_rx,
            lifecycle_tx,
            10.0,
            5.0,
            Arc::clone(&metrics),
            Arc::clone(&kill_switch),
            portfolio_manager,
            Arc::new(MockExecutionPort::default()),
        );

        dec_tx.send(make_approved_decision()).unwrap();
        drop(dec_tx);

        let handle = manager.start(0);
        let event = lifecycle_rx.recv_timeout(std::time::Duration::from_millis(200)).unwrap();
        assert!(matches!(event.event_type, OrderLifecycleEventType::Submitted));
        let _ = handle.join();
    }

    #[test]
    fn test_execution_manager_rejects_on_killswitch() {
        let (dec_tx, dec_rx) = bounded::<RiskDecision>(100);
        let (lifecycle_tx, lifecycle_rx) = bounded::<OrderLifecycleEvent>(100);
        let kill_switch = Arc::new(KillSwitch::new());
        kill_switch.activate();
        let metrics = Arc::new(GlobalMetrics::new());
        let portfolio_manager = Arc::new(PortfolioManager::new(100_000.0, 0.001));

        let manager = make_manager(
            dec_rx,
            lifecycle_tx,
            10.0,
            5.0,
            Arc::clone(&metrics),
            Arc::clone(&kill_switch),
            portfolio_manager,
            Arc::new(MockExecutionPort::default()),
        );

        dec_tx.send(make_approved_decision()).unwrap();
        drop(dec_tx);

        let handle = manager.start(0);
        assert!(lifecycle_rx.recv_timeout(std::time::Duration::from_millis(100)).is_err());
        let _ = handle.join();
    }

    #[test]
    fn test_execution_manager_on_fill() {
        let (dec_tx, dec_rx) = bounded::<RiskDecision>(100);
        let (lifecycle_tx, _lifecycle_rx) = bounded::<OrderLifecycleEvent>(100);
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        let portfolio_manager = Arc::new(PortfolioManager::new(100_000.0, 0.001));

        let mut manager = make_manager(
            dec_rx,
            lifecycle_tx,
            10.0,
            5.0,
            Arc::clone(&metrics),
            Arc::clone(&kill_switch),
            Arc::clone(&portfolio_manager),
            Arc::new(MockExecutionPort::default()),
        );

        let order_id = {
            let mut tracker = manager.order_tracker.lock();
            tracker.create_order("AAPL", "buy", 10.0, None, "corr-1")
        };
        manager.on_fill(&order_id, "AAPL", 10.0, 150.0, 42);
        assert_eq!(metrics.orders_filled.load(Ordering::Relaxed), 1);

        let pos = portfolio_manager.get_position("AAPL").unwrap();
        assert_eq!(pos.net_position, 10.0);

        drop(dec_tx);
    }

    #[test]
    fn test_execution_manager_circuit_breaker() {
        let (dec_tx, dec_rx) = bounded::<RiskDecision>(100);
        let (lifecycle_tx, _lifecycle_rx) = bounded::<OrderLifecycleEvent>(100);
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        let portfolio_manager = Arc::new(PortfolioManager::new(100_000.0, 0.001));

        let mut manager = make_manager(
            dec_rx,
            lifecycle_tx,
            100.0,
            100.0,
            Arc::clone(&metrics),
            Arc::clone(&kill_switch),
            portfolio_manager,
            Arc::new(MockExecutionPort::default()),
        );

        for _ in 0..5 {
            manager.circuit_breaker.record_failure();
        }
        assert!(manager.circuit_breaker.is_open.load(Ordering::Relaxed));

        dec_tx.send(make_approved_decision()).unwrap();
        drop(dec_tx); // drop sender so recv_timeout gets Disconnected and loop exits
        manager.run_loop();

        assert!(metrics.circuit_breaker_trips.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn test_execution_manager_retry_transient_then_success() {
        let (dec_tx, dec_rx) = bounded::<RiskDecision>(100);
        let (lifecycle_tx, lifecycle_rx) = bounded::<OrderLifecycleEvent>(100);
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        let portfolio_manager = Arc::new(PortfolioManager::new(100_000.0, 0.001));

        // Mock that fails twice transiently then succeeds
        let mock_port = Arc::new(MockExecutionPort::default());
        mock_port.set_transient_failures(2);

        let mut manager = make_manager(
            dec_rx,
            lifecycle_tx,
            10.0,
            5.0,
            Arc::clone(&metrics),
            Arc::clone(&kill_switch),
            portfolio_manager,
            mock_port.clone(),
        );

        dec_tx.send(make_approved_decision()).unwrap();
        drop(dec_tx);

        // run_loop will execute the decision with retry
        manager.run_loop();

        // After 2 transient failures + 1 success = 3 submit calls total
        // The retry loop should succeed on 3rd attempt
        let event = lifecycle_rx.recv_timeout(std::time::Duration::from_millis(300)).unwrap();
        assert!(matches!(event.event_type, OrderLifecycleEventType::Submitted));
        assert_eq!(mock_port.submit_count(), 3);
        assert_eq!(mock_port.transient_failures_remaining(), 0);
        assert_eq!(metrics.orders_submitted.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_execution_manager_retry_exhausted() {
        let (dec_tx, dec_rx) = bounded::<RiskDecision>(100);
        let (lifecycle_tx, _lifecycle_rx) = bounded::<OrderLifecycleEvent>(100);
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        let portfolio_manager = Arc::new(PortfolioManager::new(100_000.0, 0.001));

        // Mock that fails 5 times transiently (more than max 3 retries)
        let mock_port = Arc::new(MockExecutionPort::default());
        mock_port.set_transient_failures(5);

        let mut manager = make_manager(
            dec_rx,
            lifecycle_tx,
            10.0,
            5.0,
            Arc::clone(&metrics),
            Arc::clone(&kill_switch),
            portfolio_manager,
            mock_port.clone(),
        );

        dec_tx.send(make_approved_decision()).unwrap();
        drop(dec_tx);

        manager.run_loop();

        // After 3 retry attempts + 0 success = 3 submit calls, all failed
        assert_eq!(mock_port.submit_count(), 3);
        assert!(mock_port.transient_failures_remaining() > 0); // Still have failures left
        assert_eq!(metrics.orders_rejected.load(Ordering::Relaxed), 1);
    }
}
