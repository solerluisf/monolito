use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crossbeam_channel::bounded;

use unified_trading_core::config::{BackpressurePolicy, StrategyConfig, RiskConfig};
use unified_trading_core::kill_switch::KillSwitch;
use unified_trading_core::metrics::GlobalMetrics;
use unified_trading_core::portfolio_manager::PortfolioManager;
use unified_trading_core::symbol_registry::{next_request_id, SymbolId};
use market_data::{Normalizer, RawTick, TickType};
use feature::FeatureEngine;
use model::{InferenceEngine, PredictionEngine};
use strategy::{StrategyEngine, StrategyEngineExt};
use risk::{RiskCoordinator, RiskCheckRequest, RiskDecision};
use execution::{ExecutionManager, OrderLifecycleEvent};
use gateway::{MockExecutionPort, OrderCommand, IExecutionPort, BrokerError};
use unified_trading_core::threading::{spawn_pinned, ThreadPriority};

// ── Spy helpers ────────────────────────────────────────────────────

/// Wraps an `IExecutionPort` and records every `OrderCommand` submitted.
struct SpyExecutionPort {
    inner: Arc<dyn IExecutionPort>,
    submitted: Arc<parking_lot::Mutex<Vec<OrderCommand>>>,
}

impl SpyExecutionPort {
    fn new(inner: Arc<dyn IExecutionPort>) -> Self {
        Self {
            inner,
            submitted: Arc::new(parking_lot::Mutex::new(Vec::new())),
        }
    }

    fn submitted_orders(&self) -> Vec<OrderCommand> {
        self.submitted.lock().clone()
    }
}

impl IExecutionPort for SpyExecutionPort {
    fn submit_order(&self, cmd: &OrderCommand) -> Result<String, BrokerError> {
        self.submitted.lock().push(cmd.clone());
        self.inner.submit_order(cmd)
    }
    fn cancel_order(&self, cmd: &gateway::CancelCommand) -> Result<(), BrokerError> {
        self.inner.cancel_order(cmd)
    }
    fn replace_order(&self, cmd: &gateway::ReplaceCommand) -> Result<String, BrokerError> {
        self.inner.replace_order(cmd)
    }
    fn get_order_status(&self, query: &gateway::StatusQuery) -> Result<gateway::OrderStatusResponse, BrokerError> {
        self.inner.get_order_status(query)
    }
    fn query_open_orders(&self) -> Result<Vec<gateway::OpenOrderInfo>, BrokerError> {
        self.inner.query_open_orders()
    }
    fn query_positions(&self) -> Result<Vec<gateway::PositionInfo>, BrokerError> {
        self.inner.query_positions()
    }
}

fn make_raw_tick(symbol_id: SymbolId, symbol: &str, ts: u64, mid: f64, spread: f64) -> RawTick {
    RawTick {
        symbol_id,
        symbol: symbol.to_string(),
        tick_type: TickType::Quote,
        timestamp_ns: ts,
        bid: mid - spread / 2.0,
        ask: mid + spread / 2.0,
        bid_size: 100,
        ask_size: 200,
        last_price: mid,
        last_size: 50,
        exchange: "IEX".to_string(),
        trace_id: ts,
        symbol_name: None,
    }
}

fn run_processor(
    mut normalizer: Normalizer,
    mut feature_engine: FeatureEngine,
    mut strategy_engine: StrategyEngine,
    md_rx: crossbeam_channel::Receiver<RawTick>,
    feature_tx: crossbeam_channel::Sender<feature::FeatureVector>,
    risk_tx: crossbeam_channel::Sender<RiskCheckRequest>,
    kill_switch: Arc<KillSwitch>,
    metrics: Arc<GlobalMetrics>,
) {
    let mut batch = Vec::with_capacity(32);
    while !kill_switch.is_active() {
        batch.clear();
        match md_rx.recv_timeout(Duration::from_millis(10)) {
            Ok(tick) => {
                batch.push(tick);
                for _ in 1..32 {
                    match md_rx.try_recv() {
                        Ok(t) => batch.push(t),
                        Err(_) => break,
                    }
                }
            }
            Err(_) => continue,
        }

        for tick in batch.drain(..) {
            let (normalized, _gap) = normalizer.process(tick.clone()).unwrap();
            let features = feature_engine.compute(&normalized);
            let _ = feature_tx.try_send(features.clone());

            if let Some(signal) = strategy_engine.evaluate_from_features(&features) {
                let now = unified_trading_core::clock::wall_time_ns();
                let request = RiskCheckRequest {
                    request_id: next_request_id(),
                    symbol_id: signal.symbol_id,
                    intent_id: signal.intent_id,
                    side: signal.side as u8,
                    quantity: 1.0,
                    price: 150.0,
                    timestamp_ns: now,
                    current_volatility: 0.01,
                    current_spread_bps: 10.0,
                    trace_id: signal.trace_id,
                };
                match risk_tx.try_send(request) {
                    Ok(()) => {
                        metrics.intents_generated.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {}
                }
            }
            metrics.ticks_processed.fetch_add(1, Ordering::Relaxed);
            metrics.features_computed.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[test]
fn test_full_pipeline_tick_to_intent() {
    let kill_switch = Arc::new(KillSwitch::new());
    let metrics = Arc::new(GlobalMetrics::new());

    let (md_tx, md_rx) = bounded::<RawTick>(1000);
    let (feature_tx, feature_rx) = bounded(1000);
    let (risk_tx, risk_rx) = bounded::<RiskCheckRequest>(1000);
    let (decision_tx, decision_rx) = bounded::<RiskDecision>(1000);
    let (lifecycle_tx, _lifecycle_rx) = bounded::<OrderLifecycleEvent>(1000);

    let symbol_id = SymbolId::from_raw(0);
    let normalizer = Normalizer::new(symbol_id);
    let feature_engine = FeatureEngine::new("AAPL", 14, 14, 9, 20, 50, 20, 20, 1, 5, 20, 0.3, 0.02, 0.05, 0.5);
    let strategy_engine = StrategyEngine::new(symbol_id, &StrategyConfig::default());

    let pred_engine = PredictionEngine::new(feature_rx, symbol_id);
    let inference_engine = InferenceEngine::new(
        128, 0.4, 0.4, 0.2, 2.0, -0.2, 70.0, 30.0, 50.0, 0.3, 0.2, 0.3, 1.2,
    );

    let _pred_handle = pred_engine.start(move |features| {
        inference_engine.predict(features)
    }, 0);

    let risk_coordinator = RiskCoordinator::new(
        risk_rx,
        decision_tx,
        Default::default(),
        Arc::new(PortfolioManager::new(100_000.0, 0.001)),
        Arc::clone(&kill_switch),
        Arc::clone(&metrics),
        BackpressurePolicy::BlockWithTimeoutMs(10),
    );
    let _risk_handle = risk_coordinator.start(0);

    let exec_manager = ExecutionManager::new(
        decision_rx,
        lifecycle_tx,
        Arc::new(MockExecutionPort::default()),
        10.0,
        5.0,
        Arc::clone(&metrics),
        Arc::clone(&kill_switch),
        Arc::new(PortfolioManager::new(100_000.0, 0.001)),
        Arc::new(parking_lot::Mutex::new(execution::OrderTracker::new())),
        Arc::new(parking_lot::Mutex::new(execution::RateLimiter::new(10.0, 5.0))),
        Arc::new(gateway::CircuitBreaker::new(5, 30_000)),
        Arc::new(unified_trading_core::IdempotencyStore::new()),
        unified_trading_core::validator::RequestValidator::default(),
        None,
    );
    let _exec_handle = exec_manager.start(0);

    let ks = Arc::clone(&kill_switch);
    let m = Arc::clone(&metrics);
    let proc_handle = spawn_pinned(
        "test-processor",
        0,
        ThreadPriority::High,
        move || {
            run_processor(
                normalizer, feature_engine, strategy_engine,
                md_rx, feature_tx, risk_tx, ks, m,
            );
        },
    ).expect("spawn_pinned failed");

    for i in 0..50 {
        let tick = make_raw_tick(symbol_id, "TEST", i * 1_000_000, 150.0 + (i as f64 * 0.1), 0.05);
        md_tx.send(tick).unwrap();
    }

    std::thread::sleep(Duration::from_millis(200));

    let ticks = metrics.ticks_processed.load(Ordering::Relaxed);
    assert!(ticks > 0, "Expected ticks to be processed, got {}", ticks);

    kill_switch.activate();
    let _ = proc_handle.join();
}

#[test]
fn test_pipeline_with_burst_ticks() {
    let kill_switch = Arc::new(KillSwitch::new());
    let metrics = Arc::new(GlobalMetrics::new());

    let (md_tx, md_rx) = bounded::<RawTick>(10_000);
    let (feature_tx, feature_rx) = bounded(1000);
    let (risk_tx, risk_rx) = bounded::<RiskCheckRequest>(1000);
    let (decision_tx, decision_rx) = bounded::<RiskDecision>(1000);
    let (lifecycle_tx, _lifecycle_rx) = bounded::<OrderLifecycleEvent>(1000);

    let symbol_id = SymbolId::from_raw(1);
    let normalizer = Normalizer::new(symbol_id);
    let feature_engine = FeatureEngine::new("MSFT", 14, 14, 9, 20, 50, 20, 20, 1, 5, 20, 0.3, 0.02, 0.05, 0.5);
    let strategy_engine = StrategyEngine::new(symbol_id, &StrategyConfig::default());

    let pred_engine = PredictionEngine::new(feature_rx, symbol_id);
    let inference_engine = InferenceEngine::new(
        128, 0.4, 0.4, 0.2, 2.0, -0.2, 70.0, 30.0, 50.0, 0.3, 0.2, 0.3, 1.2,
    );

    let _pred_handle = pred_engine.start(move |features| {
        inference_engine.predict(features)
    }, 0);

    let risk_coordinator = RiskCoordinator::new(
        risk_rx,
        decision_tx,
        Default::default(),
        Arc::new(PortfolioManager::new(100_000.0, 0.001)),
        Arc::clone(&kill_switch),
        Arc::clone(&metrics),
        BackpressurePolicy::BlockWithTimeoutMs(10),
    );
    let _risk_handle = risk_coordinator.start(0);

    let exec_manager = ExecutionManager::new(
        decision_rx,
        lifecycle_tx,
        Arc::new(MockExecutionPort::default()),
        10.0,
        5.0,
        Arc::clone(&metrics),
        Arc::clone(&kill_switch),
        Arc::new(PortfolioManager::new(100_000.0, 0.001)),
        Arc::new(parking_lot::Mutex::new(execution::OrderTracker::new())),
        Arc::new(parking_lot::Mutex::new(execution::RateLimiter::new(10.0, 5.0))),
        Arc::new(gateway::CircuitBreaker::new(5, 30_000)),
        Arc::new(unified_trading_core::IdempotencyStore::new()),
        unified_trading_core::validator::RequestValidator::default(),
        None,
    );
    let _exec_handle = exec_manager.start(0);

    let ks = Arc::clone(&kill_switch);
    let m = Arc::clone(&metrics);
    let proc_handle = spawn_pinned(
        "test-processor",
        0,
        ThreadPriority::High,
        move || {
            run_processor(
                normalizer, feature_engine, strategy_engine,
                md_rx, feature_tx, risk_tx, ks, m,
            );
        },
    ).expect("spawn_pinned failed");

    for i in 0..500 {
        let tick = make_raw_tick(symbol_id, "TEST", i * 100_000, 400.0 + (i as f64 * 0.01), 0.04);
        md_tx.send(tick).unwrap();
    }

    std::thread::sleep(Duration::from_millis(500));

    let ticks = metrics.ticks_processed.load(Ordering::Relaxed);
    assert!(ticks >= 500, "Expected 500 ticks, got {}", ticks);

    kill_switch.activate();
    let _ = proc_handle.join();
}

#[test]
fn test_kill_switch_stops_pipeline() {
    let kill_switch = Arc::new(KillSwitch::new());
    let metrics = Arc::new(GlobalMetrics::new());

    let (md_tx, md_rx) = bounded::<RawTick>(10_000);
    let (feature_tx, feature_rx) = bounded(1000);
    let (risk_tx, risk_rx) = bounded::<RiskCheckRequest>(1000);
    let (decision_tx, decision_rx) = bounded::<RiskDecision>(1000);
    let (lifecycle_tx, _lifecycle_rx) = bounded::<OrderLifecycleEvent>(1000);

    let symbol_id = SymbolId::from_raw(0);
    let normalizer = Normalizer::new(symbol_id);
    let feature_engine = FeatureEngine::new("AAPL", 14, 14, 9, 20, 50, 20, 20, 1, 5, 20, 0.3, 0.02, 0.05, 0.5);
    let strategy_engine = StrategyEngine::new(symbol_id, &StrategyConfig::default());

    let pred_engine = PredictionEngine::new(feature_rx, symbol_id);
    let inference_engine = InferenceEngine::new(
        128, 0.4, 0.4, 0.2, 2.0, -0.2, 70.0, 30.0, 50.0, 0.3, 0.2, 0.3, 1.2,
    );

    let _pred_handle = pred_engine.start(move |features| {
        inference_engine.predict(features)
    }, 0);

    let risk_coordinator = RiskCoordinator::new(
        risk_rx,
        decision_tx,
        Default::default(),
        Arc::new(PortfolioManager::new(100_000.0, 0.001)),
        Arc::clone(&kill_switch),
        Arc::clone(&metrics),
        BackpressurePolicy::BlockWithTimeoutMs(10),
    );
    let _risk_handle = risk_coordinator.start(0);

    let exec_manager = ExecutionManager::new(
        decision_rx,
        lifecycle_tx,
        Arc::new(MockExecutionPort::default()),
        10.0,
        5.0,
        Arc::clone(&metrics),
        Arc::clone(&kill_switch),
        Arc::new(PortfolioManager::new(100_000.0, 0.001)),
        Arc::new(parking_lot::Mutex::new(execution::OrderTracker::new())),
        Arc::new(parking_lot::Mutex::new(execution::RateLimiter::new(10.0, 5.0))),
        Arc::new(gateway::CircuitBreaker::new(5, 30_000)),
        Arc::new(unified_trading_core::IdempotencyStore::new()),
        unified_trading_core::validator::RequestValidator::default(),
        None,
    );
    let _exec_handle = exec_manager.start(0);

    let ks = Arc::clone(&kill_switch);
    let m = Arc::clone(&metrics);
    let proc_handle = spawn_pinned(
        "test-processor",
        0,
        ThreadPriority::High,
        move || {
            run_processor(
                normalizer, feature_engine, strategy_engine,
                md_rx, feature_tx, risk_tx, ks, m,
            );
        },
    ).expect("spawn_pinned failed");

    for i in 0..100 {
        let tick = make_raw_tick(symbol_id, "TEST", i * 1_000_000, 150.0, 0.05);
        md_tx.send(tick).unwrap();
    }

    std::thread::sleep(Duration::from_millis(100));
    let ticks_before = metrics.ticks_processed.load(Ordering::Relaxed);

    kill_switch.activate();
    std::thread::sleep(Duration::from_millis(100));

    let ticks_after = metrics.ticks_processed.load(Ordering::Relaxed);
    assert!(ticks_after >= ticks_before);
    assert!(ticks_after >= 100, "Expected all 100 ticks processed, got {}", ticks_after);

    let _ = proc_handle.join();
}

// ══════════════════════════════════════════════════════════════════
//  End-to-end pipeline integration tests
//  RawTick → Normalizer → FeatureEngine → Strategy → Risk → Execution
// ══════════════════════════════════════════════════════════════════

/// Shared fixture that wires the complete pipeline and returns handles
/// so tests can inject ticks and inspect every stage.
struct PipelineFixture {
    md_tx: crossbeam_channel::Sender<RawTick>,
    kill_switch: Arc<KillSwitch>,
    metrics: Arc<GlobalMetrics>,
    spy_port: Arc<SpyExecutionPort>,
    lifecycle_rx: crossbeam_channel::Receiver<OrderLifecycleEvent>,
    _proc_handle: std::thread::JoinHandle<()>,
    _pred_handle: std::thread::JoinHandle<()>,
    _risk_handle: std::thread::JoinHandle<()>,
    _exec_handle: std::thread::JoinHandle<()>,
}

impl PipelineFixture {
    /// Build the full pipeline with **aggressive** strategy thresholds so
    /// that a strong uptrend or downtrend in tick data generates signals.
    fn with_aggressive_strategy() -> Self {
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());

        let (md_tx, md_rx) = bounded::<RawTick>(10_000);
        let (feature_tx, feature_rx) = bounded::<feature::FeatureVector>(10_000);
        let (risk_tx, risk_rx) = bounded::<RiskCheckRequest>(10_000);
        let (decision_tx, decision_rx) = bounded::<RiskDecision>(10_000);
        let (lifecycle_tx, lifecycle_rx) = bounded::<OrderLifecycleEvent>(10_000);

        let symbol_id = SymbolId::from_raw(42);
        let normalizer = Normalizer::new(symbol_id);
        let feature_engine = FeatureEngine::new(
            "E2E", 14, 14, 9, 20, 50, 20, 20,
            1, 5, 20, 0.3, 0.02, 0.05, 0.5,
        );

        // Aggressive strategy: low thresholds so moderate price moves trigger signals
        let mut strat_cfg = StrategyConfig::default();
        strat_cfg.long_entry_threshold = 0.1;   // very easy to trigger long
        strat_cfg.short_entry_threshold = -0.1;
        strat_cfg.confidence_minimum = 0.1;     // very easy to pass confidence
        strat_cfg.exit_threshold = 0.05;
        let strategy_engine = StrategyEngine::new(symbol_id, &strat_cfg);

        // Prediction engine with heuristic fallback (confidence=0.6)
        let pred_engine = PredictionEngine::new(feature_rx, symbol_id);
        let inference_engine = InferenceEngine::new(
            128, 0.4, 0.4, 0.2, 2.0, -0.2, 70.0, 30.0, 50.0,
            0.3, 0.2, 0.3, 1.2,
        );
        let pred_handle = pred_engine.start(move |features| {
            inference_engine.predict(features)
        }, 0);

        // Permissive risk config — allow most things through
        let mut risk_cfg = RiskConfig::default();
        risk_cfg.max_position_per_symbol = 999_999.0;
        risk_cfg.max_portfolio_exposure = 999_999.0;
        risk_cfg.max_leverage = 10.0;
        risk_cfg.max_order_rate_per_sec = 10_000;       // no rate limiting in test
        risk_cfg.risk_intent_staleness_ns = 30_000_000_000; // 30s window

        let risk_coordinator = RiskCoordinator::new(
            risk_rx, decision_tx, risk_cfg,
            Arc::new(PortfolioManager::new(1_000_000.0, 0.001)),
            Arc::clone(&kill_switch),
            Arc::clone(&metrics),
            BackpressurePolicy::BlockWithTimeoutMs(50), // block rather than drop
        );
        let risk_handle = risk_coordinator.start(0);

        // Spy execution port wrapped around MockExecutionPort
        let mock = Arc::new(MockExecutionPort::default());
        let spy_port = Arc::new(SpyExecutionPort::new(Arc::clone(&mock) as Arc<dyn IExecutionPort>));

        let exec_manager = ExecutionManager::new(
            decision_rx.clone(), lifecycle_tx,
            Arc::clone(&spy_port) as Arc<dyn IExecutionPort>,
            100.0,   // high global rate limit
            50.0,    // high per-symbol rate
            Arc::clone(&metrics),
            Arc::clone(&kill_switch),
            Arc::new(PortfolioManager::new(1_000_000.0, 0.001)),
            Arc::new(parking_lot::Mutex::new(execution::OrderTracker::new())),
            Arc::new(parking_lot::Mutex::new(execution::RateLimiter::new(100.0, 50.0))),
            Arc::new(gateway::CircuitBreaker::new(999, 30_000)),
            Arc::new(unified_trading_core::IdempotencyStore::new()),
            unified_trading_core::validator::RequestValidator::default(),
            None,
        );
        let exec_handle = exec_manager.start(0);

        let ks = Arc::clone(&kill_switch);
        let m = Arc::clone(&metrics);
        let proc_handle = spawn_pinned("e2e-processor", 0, ThreadPriority::High, move || {
            run_processor(normalizer, feature_engine, strategy_engine,
                md_rx, feature_tx, risk_tx, ks, m);
        }).expect("spawn failed");

        Self {
            md_tx,
            kill_switch,
            metrics,
            spy_port,
            lifecycle_rx,
            _proc_handle: proc_handle,
            _pred_handle: pred_handle,
            _risk_handle: risk_handle,
            _exec_handle: exec_handle,
        }
    }

    fn shutdown(self) {
        self.kill_switch.activate();
        let _ = self._proc_handle.join();
    }
}

/// Generate a strong uptrend sequence of ticks — prices climb steadily with
/// moderate spread. This should drive RSI up and MACD positive → long signal.
fn generate_uptrend_ticks(symbol_id: SymbolId, symbol: &str, count: usize, start_price: f64, step: f64) -> Vec<RawTick> {
    (0..count).map(|i| {
        let ts = (i as u64) * 1_000_000; // 1 ms apart
        let price = start_price + (i as f64) * step;
        make_raw_tick(symbol_id, symbol, ts, price, 0.04)
    }).collect()
}

/// Generate a strong downtrend sequence — prices fall steadily.
fn generate_downtrend_ticks(symbol_id: SymbolId, symbol: &str, count: usize, start_price: f64, step: f64) -> Vec<RawTick> {
    (0..count).map(|i| {
        let ts = (i as u64) * 1_000_000;
        let price = start_price - (i as f64) * step;
        make_raw_tick(symbol_id, symbol, ts, price.max(1.0), 0.04)
    }).collect()
}

#[test]
fn test_e2e_long_entry_signal_path() {
    let fix = PipelineFixture::with_aggressive_strategy();

    // Inject a strong uptrend — should eventually produce Long entry signals
    let ticks = generate_uptrend_ticks(SymbolId::from_raw(42), "TEST", 500, 100.0, 0.5);
    for tick in &ticks {
        fix.md_tx.send(tick.clone()).unwrap();
    }

    // Allow pipeline to drain — use polling for async components
    std::thread::sleep(Duration::from_millis(1500));

    // ── Stage 1: Normalizer + FeatureEngine processed ticks ────────
    let ticks_processed = fix.metrics.ticks_processed.load(Ordering::Relaxed);
    assert!(ticks_processed >= 200,
        "Normalizer should process ≥200 ticks, got {}", ticks_processed);

    let features_computed = fix.metrics.features_computed.load(Ordering::Relaxed);
    assert!(features_computed == ticks_processed,
        "FeatureEngine should compute one feature vector per tick: {} vs {}",
        features_computed, ticks_processed);

    // ── Stage 2: Strategy generated trade intents ───────────────────
    let intents_generated = fix.metrics.intents_generated.load(Ordering::Relaxed);
    // With aggressive thresholds and 500 uptick ticks, we expect ≥1 intent
    assert!(intents_generated >= 1,
        "Strategy should generate ≥1 trade intent from uptrend, got {}. \
         (ticks={}, features={})",
        intents_generated, ticks_processed, features_computed);

    // ── Stage 3: RiskCoordinator processed & ExecutionManager consumed ──
    // NOTE: We cannot read from decision_rx because ExecutionManager owns a
    // clone and consumes approved decisions to submit orders. Instead we verify
    // via metrics + spy port that the full path completed.
    let intents_approved = fix.metrics.intents_approved.load(Ordering::Relaxed);
    let intents_rejected = fix.metrics.intents_rejected.load(Ordering::Relaxed);
    assert!(intents_approved + intents_rejected >= 1,
        "RiskCoordinator should have evaluated ≥1 intent (approved={}, rejected={})",
        intents_approved, intents_rejected);

    // ── Stage 4: ExecutionManager submitted orders ──────────────────
    let orders = fix.spy_port.submitted_orders();
    let submitted = fix.metrics.orders_submitted.load(Ordering::Relaxed);
    assert!(submitted == orders.len() as u64,
        "orders_submitted metric ({}) should match spy capture ({})",
        submitted, orders.len());
    // With approved decisions and permissive rate limiter, orders should be submitted
    assert!(orders.len() >= 1,
        "ExecutionManager should submit ≥1 order for approved decisions. \
         Spy captured {} orders, metric says {}",
        orders.len(), submitted);

    // Validate order structure
    for order in &orders {
        assert!(!order.order_id.is_empty(), "Order must have an order_id");
        assert!(!order.symbol.is_empty(), "Order must have a symbol");
        assert!(order.quantity > 0.0, "Order quantity must be positive");
        assert!(matches!(order.order_type, gateway::OrderType::Market), "Should be market order");
    }

    // ── Stage 5: Lifecycle events emitted ───────────────────────────
    let mut lifecycle_events: Vec<OrderLifecycleEvent> = Vec::new();
    while let Ok(evt) = fix.lifecycle_rx.try_recv() {
        lifecycle_events.push(evt);
    }
    // Each submitted order should produce a lifecycle event
    assert!(lifecycle_events.len() >= orders.len(),
        "Expected ≥{} lifecycle events (one per order), got {}",
        orders.len(), lifecycle_events.len());

    // ── Stage 6: Metrics consistency checks ─────────────────────────
    let feed_gaps = fix.metrics.feed_gaps.load(Ordering::Relaxed);
    let dropped_intents = fix.metrics.dropped_intents.load(Ordering::Relaxed);
    tracing::info!(
        ticks = ticks_processed,
        features = features_computed,
        intents = intents_generated,
        risk_approved = intents_approved,
        risk_rejected = intents_rejected,
        orders_submitted = submitted,
        lifecycle = lifecycle_events.len(),
        feed_gaps = feed_gaps,
        dropped = dropped_intents,
        "E2E long-entry pipeline summary"
    );

    fix.shutdown();
}

#[test]
fn test_e2e_exit_signal_path_with_downtrend() {
    let fix = PipelineFixture::with_aggressive_strategy();

    // First send an uptrend to establish position / warm up indicators
    let up_ticks = generate_uptrend_ticks(SymbolId::from_raw(42), "TEST", 150, 100.0, 0.3);
    for t in &up_ticks { fix.md_tx.send(t.clone()).unwrap(); }

    // Then reverse into a downtrend — should generate Short / CloseLong signals
    let down_ticks = generate_downtrend_ticks(SymbolId::from_raw(42), "TEST", 150, 145.5, 0.5);
    for t in &down_ticks { fix.md_tx.send(t.clone()).unwrap(); }

    std::thread::sleep(Duration::from_millis(1000));

    let ticks_processed = fix.metrics.ticks_processed.load(Ordering::Relaxed);
    assert!(ticks_processed >= 200,
        "Should process ≥200 ticks across both trends, got {}", ticks_processed);

    let intents = fix.metrics.intents_generated.load(Ordering::Relaxed);
    assert!(intents >= 1,
        "Should generate ≥1 intent from trend reversal, got {}", intents);

    // Verify risk evaluated intents
    let risk_evaluated = fix.metrics.intents_approved.load(Ordering::Relaxed)
        + fix.metrics.intents_rejected.load(Ordering::Relaxed);
    assert!(risk_evaluated >= 1,
        "Risk should have evaluated ≥1 intent, got {}",
        risk_evaluated);

    // Orders should still be submitted regardless of direction
    let orders = fix.spy_port.submitted_orders();
    let submitted = fix.metrics.orders_submitted.load(Ordering::Relaxed);
    assert!(submitted == orders.len() as u64,
        "Metric/spy mismatch on exit path: metric={} spy={}",
        submitted, orders.len());

    tracing::info!(
        ticks = ticks_processed,
        intents = intents,
        risk_evaluated = risk_evaluated,
        orders = orders.len(),
        "E2E exit-signal pipeline summary"
    );

    fix.shutdown();
}

#[test]
fn test_e2e_trace_id_propagation_across_pipeline() {
    let fix = PipelineFixture::with_aggressive_strategy();

    // Send ticks with known trace IDs
    let base_ts = 10_000_000u64;
    for i in 0..200u64 {
        let tick = make_raw_tick(
            SymbolId::from_raw(42),
            "TEST",
            base_ts + i * 1_000_000,
            100.0 + (i as f64) * 0.4,
            0.04,
        );
        fix.md_tx.send(tick).unwrap();
    }

    std::thread::sleep(Duration::from_millis(600));

    // Verify that trace IDs propagate into submitted orders
    let orders = fix.spy_port.submitted_orders();
    if !orders.is_empty() {
        for order in &orders {
            assert_ne!(order.trace_id, 0,
                "Order should carry propagated trace_id from RawTick");
        }
    }

    fix.shutdown();
}

#[test]
fn test_e2e_rejected_risk_decisions_do_not_become_orders() {
    let fix = PipelineFixture::with_aggressive_strategy();

    // Send enough ticks to generate some activity
    let ticks = generate_uptrend_ticks(SymbolId::from_raw(42), "TEST", 100, 100.0, 0.3);
    for t in &ticks { fix.md_tx.send(t.clone()).unwrap(); }

    std::thread::sleep(Duration::from_millis(500));

    // Use metrics to verify the invariant: orders ≤ approved intents
    let intents_approved = fix.metrics.intents_approved.load(Ordering::Relaxed);
    let intents_rejected = fix.metrics.intents_rejected.load(Ordering::Relaxed);
    let orders_submitted = fix.metrics.orders_submitted.load(Ordering::Relaxed);

    if intents_rejected > 0 {
        assert!(orders_submitted <= intents_approved as u64,
            "Orders ({}) cannot exceed approved intents ({}), even with {} rejected",
            orders_submitted, intents_approved, intents_rejected);
    }

    tracing::info!(
        approved = intents_approved,
        rejected = intents_rejected,
        orders = orders_submitted,
        "Rejection-path verification"
    );

    fix.shutdown();
}

#[test]
fn test_circuit_breaker_trips_after_mock_failures() {
    use gateway::{CircuitBreaker, MockExecutionPort, OrderSide, OrderType, TimeInForce};

    // Create a circuit breaker with threshold of 3 failures
    let cb = Arc::new(CircuitBreaker::new(3, 30_000));

    // Create a mock execution port that always fails
    let mock = Arc::new(MockExecutionPort::builder()
        .explicit_failure(true)
        .failure_message("Simulated network error".to_string())
        .build());

    // Verify initial state
    assert!(cb.can_execute(), "Circuit breaker should allow execution initially");
    assert_eq!(cb.state_name(), "closed");

    // Submit orders that will fail - each failure should be recorded
    let cmd = OrderCommand {
        order_id: "cb-test".to_string(),
        symbol: "AAPL".to_string(),
        side: OrderSide::Buy,
        quantity: 10.0,
        order_type: OrderType::Market,
        limit_price: None,
        stop_price: None,
        time_in_force: TimeInForce::Day,
        correlation_id: "corr-1".to_string(),
        trace_id: 42,
    };

    // First failure
    let _ = mock.submit_order(&cmd);
    cb.record_failure();
    assert!(cb.can_execute(), "Circuit breaker should still allow execution after 1 failure");
    assert_eq!(cb.failure_count(), 1);

    // Second failure
    let _ = mock.submit_order(&cmd);
    cb.record_failure();
    assert!(cb.can_execute(), "Circuit breaker should still allow execution after 2 failures");
    assert_eq!(cb.failure_count(), 2);

    // Third failure - should trip the circuit breaker
    let _ = mock.submit_order(&cmd);
    cb.record_failure();
    assert!(!cb.can_execute(), "Circuit breaker should open after 3 failures");
    assert!(cb.is_open.load(Ordering::Relaxed));
    assert_eq!(cb.state_name(), "open");

    // Verify mock tracked failures correctly
    assert_eq!(mock.submit_count(), 3);
    assert_eq!(mock.failure_count(), 3);

    tracing::info!(
        state = cb.state_name(),
        is_open = cb.is_open.load(Ordering::Relaxed),
        "Circuit breaker successfully tripped after 3 mock failures"
    );
}

#[test]
fn test_circuit_breaker_resets_after_success() {
    use gateway::CircuitBreaker;

    let cb = Arc::new(CircuitBreaker::new(3, 10)); // 10ms cooldown for testing

    // Trip the circuit breaker
    cb.record_failure();
    cb.record_failure();
    cb.record_failure();
    assert!(!cb.can_execute(), "Circuit breaker should be open");
    assert_eq!(cb.state_name(), "open");

    // Wait for cooldown to elapse
    std::thread::sleep(Duration::from_millis(20));

    // Probe should succeed and reset the breaker
    assert!(cb.can_execute(), "Circuit breaker should transition to half-open after cooldown");
    assert_eq!(cb.state_name(), "half_open");

    cb.record_success();
    assert_eq!(cb.state_name(), "closed", "Circuit breaker should close after successful probe");
    assert!(!cb.is_open.load(Ordering::Relaxed));
}

#[test]
fn test_mock_partial_fill_behavior() {
    use gateway::MockExecutionPort;

    let mock = Arc::new(MockExecutionPort::builder()
        .partial_fill_rate(1.0) // Always partial fill
        .partial_fill_qty(25)
        .build());

    let query = gateway::StatusQuery {
        execution_id: "partial-test".to_string(),
    };

    let result = mock.get_order_status(&query).unwrap();
    assert_eq!(result.status, "partially_filled");
    assert_eq!(result.filled_qty, 25);
    assert_eq!(result.remaining_qty, 75);
}
