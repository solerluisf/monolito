use crossbeam_channel::{Receiver, Sender};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use unified_trading_core::kill_switch::KillSwitch;
use unified_trading_core::metrics::GlobalMetrics;
use unified_trading_core::journal::{JournalWriter, JournalEntry};
use unified_trading_core::validator::RequestValidator;
use unified_trading_core::idempotency::IdempotencyStore;
use unified_trading_core::portfolio_manager::PortfolioManager;
use unified_trading_core::threading::{spawn_pinned, ThreadPriority};

use crate::order_tracker::{OrderTracker, OrderStatus};
use crate::rate_limiter::RateLimiter;
use crate::order_lifecycle::{OrderLifecycleEvent, OrderLifecycleEventType};
use gateway::{CircuitBreaker, IExecutionPort, OrderCommand, OrderSide, OrderType, TimeInForce};
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
    pub journal: Option<Arc<JournalWriter>>,
    pub kill_switch: Arc<KillSwitch>,
    pub validator: RequestValidator,
    running: Arc<AtomicBool>,
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
            journal: None,
            kill_switch,
            validator,
            running: Arc::new(AtomicBool::new(true)),
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

    #[tracing::instrument(skip_all, fields(request_id = %decision.request_id))]
    fn execute_decision(&mut self, decision: &RiskDecision) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let symbol = decision.request.symbol.clone();

        if let Err(e) = self.validator.validate_symbol(&symbol) {
            tracing::warn!("Validation failed for {}: {}", symbol, e);
            self.metrics.orders_rejected.fetch_add(1, Ordering::Relaxed);
            return;
        }

        {
            let mut rate_limiter = self.rate_limiter.lock();
            if !rate_limiter.try_consume(&symbol, 1.0) {
                self.metrics.orders_rejected.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(symbol = %symbol, "Rate limit exceeded");
                return;
            }
        }

        let idempotency_key = format!("{}-{}", symbol, decision.request_id);
        if self.idempotency_store.is_processed(&idempotency_key) {
            tracing::warn!(idempotency_key = %idempotency_key, "Duplicate order detected");
            return;
        }

        let order_id = uuid::Uuid::new_v4().to_string();

        let side = match decision.request.side.as_str() {
            "Long" | "CloseShort" | "Buy" | "buy" => OrderSide::Buy,
            "Short" | "CloseLong" | "Sell" | "sell" => OrderSide::Sell,
            other => {
                tracing::warn!("Unexpected side '{}', defaulting to Sell", other);
                OrderSide::Sell
            }
        };

        let quantity = decision.request.quantity;

        let cmd = OrderCommand {
            order_id: order_id.clone(),
            symbol: symbol.clone(),
            side: side.clone(),
            quantity,
            order_type: OrderType::Market,
            limit_price: None,
            stop_price: None,
            time_in_force: TimeInForce::Day,
            correlation_id: decision.request_id.clone(),
        };

        // Persist to journal BEFORE submitting to broker for critical commands
        // This ensures we can replay/recover if needed
        if let Some(ref journal) = self.journal {
            let entry = JournalEntry::Order {
                symbol: symbol.clone(),
                timestamp_ns: now,
                data: format!("order_id={},side={:?},qty={},decision={}", order_id, side, quantity, decision.request_id),
            };
            
            // Write to journal first
            if let Err(e) = journal.write(entry) {
                tracing::error!("Failed to write to journal: {}", e);
                // Continue anyway - journal failure shouldn't block trading
            } else {
                // Sync flush for critical orders to ensure durability
                if let Err(e) = journal.flush_sync() {
                    tracing::error!("Failed to flush journal: {}", e);
                }
            }
        }

        let broker_start = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        match self.execution_port.submit_order(&cmd) {
            Ok(execution_id) => {
                let broker_end = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;
                let rtt_ns = broker_end.saturating_sub(broker_start);
                self.metrics.broker_round_trip_latency.record(rtt_ns);
                self.metrics.broker_send_latency.record(rtt_ns);

                self.metrics.orders_submitted.fetch_add(1, Ordering::Relaxed);
                self.kill_switch.track_open_order(&execution_id);

                let lifecycle_event = OrderLifecycleEvent::new(
                    execution_id.clone(),
                    symbol.clone(),
                    OrderLifecycleEventType::Submitted,
                    now,
                )
                .with_status("submitted".to_string());

                self.metrics.lifecycle_channel_depth.fetch_add(1, Ordering::Relaxed);
                let _ = self.lifecycle_tx.send(lifecycle_event);

                tracing::info!(order_id = %execution_id, symbol = %symbol, "Order submitted to broker");

                self.idempotency_store.mark_processed(idempotency_key, execution_id);
                self.circuit_breaker.record_success();
            }
            Err(e) => {
                self.metrics.orders_rejected.fetch_add(1, Ordering::Relaxed);
                self.circuit_breaker.record_failure();
                tracing::warn!(symbol = %symbol, error = %e, "Order submission failed");
                
                // Write failure to journal
                if let Some(ref journal) = self.journal {
                    let _ = journal.write(JournalEntry::Event {
                        event_type: "ORDER_FAILED".to_string(),
                        timestamp_ns: now,
                        data: format!("symbol={},error={}", symbol, e),
                    });
                }
            }
        }
    }

    pub fn on_fill(&mut self, order_id: &str, symbol: &str, filled_qty: f64, fill_price: f64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

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
        .with_status("filled".to_string());

        let _ = self.lifecycle_tx.send(lifecycle_event);

        tracing::info!(order_id = %order_id, symbol = %symbol, qty = filled_qty, price = fill_price, "Order filled");
    }

    pub fn on_reject(&mut self, order_id: &str, symbol: &str, reason: &str) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        self.kill_switch.remove_open_order(order_id);
        self.metrics.orders_rejected.fetch_add(1, Ordering::Relaxed);
        self.circuit_breaker.record_failure();

        let lifecycle_event = OrderLifecycleEvent::new(
            order_id.to_string(),
            symbol.to_string(),
            OrderLifecycleEventType::Rejected,
            now,
        )
        .with_status(format!("rejected: {}", reason));

        let _ = self.lifecycle_tx.send(lifecycle_event);
    }

    pub fn on_cancel(&mut self, order_id: &str, symbol: &str) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

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
        .with_status("cancelled".to_string());

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

use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::bounded;
    use std::time::{SystemTime, UNIX_EPOCH};
    use gateway::MockExecutionPort;

    fn make_approved_decision() -> RiskDecision {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let request = risk::RiskCheckRequest {
            request_id: "req-1".to_string(),
            symbol: "AAPL".to_string(),
            intent_id: "intent-1".to_string(),
            side: "buy".to_string(),
            quantity: 10.0,
            price: 150.0,
            timestamp_ns: now,
            current_volatility: 0.01,
            current_spread_bps: 10.0,
        };
        RiskDecision {
            request_id: request.request_id.clone(),
            approved: true,
            rejection_reason: None,
            warnings: Vec::new(),
            check_index: 14,
            timestamp_ns: now,
            request,
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
    ) -> ExecutionManager {
        let order_tracker = Arc::new(parking_lot::Mutex::new(OrderTracker::new()));
        let rate_limiter = Arc::new(parking_lot::Mutex::new(RateLimiter::new(global_rate, per_symbol_rate)));
        let circuit_breaker = Arc::new(CircuitBreaker::new(5, 30_000));
        let idempotency_store = Arc::new(IdempotencyStore::new());
        ExecutionManager::new(
            dec_rx,
            lifecycle_tx,
            Arc::new(MockExecutionPort::default()),
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
        );

        let order_id = {
            let mut tracker = manager.order_tracker.lock();
            tracker.create_order("AAPL", "buy", 10.0, None, "corr-1")
        };
        manager.on_fill(&order_id, "AAPL", 10.0, 150.0);
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
}
