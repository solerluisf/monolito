use crossbeam_channel::{Receiver, Sender};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use unified_trading_core::kill_switch::KillSwitch;
use unified_trading_core::metrics::GlobalMetrics;
use unified_trading_core::journal::{JournalWriter, JournalEntry};
use unified_trading_core::validator::RequestValidator;
use unified_trading_core::idempotency::IdempotencyStore;
use unified_trading_core::position_manager::PositionManager;

use crate::order_tracker::{OrderTracker, OrderStatus};
use crate::rate_limiter::RateLimiter;
use crate::order_lifecycle::{OrderLifecycleEvent, OrderLifecycleEventType};
use gateway::{CircuitBreaker, IExecutionPort, OrderCommand, OrderSide, OrderType, TimeInForce};
use risk::RiskDecision;

pub struct ExecutionManager {
    pub decision_rx: Receiver<RiskDecision>,
    pub lifecycle_tx: Sender<OrderLifecycleEvent>,
    pub execution_port: Arc<dyn IExecutionPort>,
    pub order_tracker: OrderTracker,
    pub rate_limiter: RateLimiter,
    pub circuit_breaker: CircuitBreaker,
    pub idempotency_store: IdempotencyStore,
    pub position_manager: Arc<PositionManager>,
    pub metrics: Arc<GlobalMetrics>,
    pub journal: Option<Arc<JournalWriter>>,
    pub kill_switch: Arc<KillSwitch>,
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
        position_manager: Arc<PositionManager>,
    ) -> Self {
        Self {
            decision_rx,
            lifecycle_tx,
            execution_port,
            order_tracker: OrderTracker::new(),
            rate_limiter: RateLimiter::new(global_rate, per_symbol_rate),
            circuit_breaker: CircuitBreaker::new(5, 30_000),
            idempotency_store: IdempotencyStore::new(),
            position_manager,
            metrics,
            journal: None,
            kill_switch,
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
    }

    #[tracing::instrument(skip_all, fields(request_id = %decision.request_id))]
    fn execute_decision(&mut self, decision: &RiskDecision) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let symbol = decision.request_id.chars().take(4).collect::<String>();

        if let Err(e) = RequestValidator::validate_symbol(&symbol) {
            tracing::warn!("Validation failed for {}: {}", symbol, e);
            self.metrics.orders_rejected.fetch_add(1, Ordering::Relaxed);
            return;
        }

        if !self.rate_limiter.try_consume(&symbol, 1.0) {
            self.metrics.orders_rejected.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(symbol = %symbol, "Rate limit exceeded");
            return;
        }

        let idempotency_key = format!("{}-{}", symbol, decision.request_id);
        if self.idempotency_store.is_processed(&idempotency_key) {
            tracing::warn!(idempotency_key = %idempotency_key, "Duplicate order detected");
            return;
        }

        let order_id = uuid::Uuid::new_v4().to_string();

        let side = if symbol.starts_with("req-") {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };

        let cmd = OrderCommand {
            order_id: order_id.clone(),
            symbol: symbol.clone(),
            side,
            quantity: 1.0,
            order_type: OrderType::Market,
            limit_price: None,
            stop_price: None,
            time_in_force: TimeInForce::Day,
            correlation_id: decision.request_id.clone(),
        };

        match self.execution_port.submit_order(&cmd) {
            Ok(execution_id) => {
                self.metrics.orders_submitted.fetch_add(1, Ordering::Relaxed);
                self.kill_switch.track_open_order(&execution_id);

                let lifecycle_event = OrderLifecycleEvent::new(
                    execution_id.clone(),
                    symbol.clone(),
                    OrderLifecycleEventType::Submitted,
                    now,
                )
                .with_status("submitted".to_string());

                let _ = self.lifecycle_tx.send(lifecycle_event);

                tracing::info!(order_id = %execution_id, symbol = %symbol, "Order submitted to broker");

                if let Some(ref journal) = self.journal {
                    let _ = journal.write(JournalEntry::Order {
                        symbol,
                        timestamp_ns: now,
                        data: format!("execution_id={},decision={}", execution_id, decision.request_id),
                    });
                }

                self.idempotency_store.mark_processed(idempotency_key, execution_id);
                self.circuit_breaker.record_success();
            }
            Err(e) => {
                self.metrics.orders_rejected.fetch_add(1, Ordering::Relaxed);
                self.circuit_breaker.record_failure();
                tracing::warn!(symbol = %symbol, error = %e, "Order submission failed");
            }
        }
    }

    pub fn on_fill(&mut self, order_id: &str, symbol: &str, filled_qty: f64, fill_price: f64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        self.order_tracker.update_fill(order_id, filled_qty);
        self.kill_switch.remove_open_order(order_id);
        self.metrics.orders_filled.fetch_add(1, Ordering::Relaxed);
        self.circuit_breaker.record_success();

        self.position_manager.on_fill(symbol, filled_qty, fill_price, true);

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

        self.order_tracker.update_status(order_id, OrderStatus::Cancelled);
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

    pub fn start(mut self) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            self.run_loop();
        })
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
        RiskDecision {
            request_id: "req-1".to_string(),
            approved: true,
            rejection_reason: None,
            check_index: 14,
            timestamp_ns: now,
        }
    }

    #[test]
    fn test_execution_manager_processes_approved() {
        let (dec_tx, dec_rx) = bounded::<RiskDecision>(100);
        let (lifecycle_tx, lifecycle_rx) = bounded::<OrderLifecycleEvent>(100);
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        let position_manager = Arc::new(PositionManager::new());

        let manager = ExecutionManager::new(
            dec_rx,
            lifecycle_tx,
            Arc::new(MockExecutionPort),
            10.0,
            5.0,
            Arc::clone(&metrics),
            Arc::clone(&kill_switch),
            position_manager,
        );

        dec_tx.send(make_approved_decision()).unwrap();
        drop(dec_tx);

        let handle = manager.start();
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
        let position_manager = Arc::new(PositionManager::new());

        let manager = ExecutionManager::new(
            dec_rx,
            lifecycle_tx,
            Arc::new(MockExecutionPort),
            10.0,
            5.0,
            Arc::clone(&metrics),
            Arc::clone(&kill_switch),
            position_manager,
        );

        dec_tx.send(make_approved_decision()).unwrap();
        drop(dec_tx);

        let handle = manager.start();
        assert!(lifecycle_rx.recv_timeout(std::time::Duration::from_millis(100)).is_err());
        let _ = handle.join();
    }

    #[test]
    fn test_execution_manager_on_fill() {
        let (dec_tx, dec_rx) = bounded::<RiskDecision>(100);
        let (lifecycle_tx, _lifecycle_rx) = bounded::<OrderLifecycleEvent>(100);
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        let position_manager = Arc::new(PositionManager::new());

        let mut manager = ExecutionManager::new(
            dec_rx,
            lifecycle_tx,
            Arc::new(MockExecutionPort),
            10.0,
            5.0,
            Arc::clone(&metrics),
            Arc::clone(&kill_switch),
            Arc::clone(&position_manager),
        );

        let order_id = manager.order_tracker.create_order("AAPL", "buy", 10.0, None, "corr-1");
        manager.on_fill(&order_id, "AAPL", 10.0, 150.0);
        assert_eq!(metrics.orders_filled.load(Ordering::Relaxed), 1);

        let pos = position_manager.get_position("AAPL").unwrap();
        assert_eq!(pos.quantity, 10.0);

        drop(dec_tx);
    }

    #[test]
    fn test_execution_manager_circuit_breaker() {
        let (dec_tx, dec_rx) = bounded::<RiskDecision>(100);
        let (lifecycle_tx, _lifecycle_rx) = bounded::<OrderLifecycleEvent>(100);
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        let position_manager = Arc::new(PositionManager::new());

        let mut manager = ExecutionManager::new(
            dec_rx,
            lifecycle_tx,
            Arc::new(MockExecutionPort),
            100.0,
            100.0,
            Arc::clone(&metrics),
            Arc::clone(&kill_switch),
            position_manager,
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
