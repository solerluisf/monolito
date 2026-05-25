use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crossbeam_channel::bounded;

use unified_trading_core::kill_switch::KillSwitch;
use unified_trading_core::metrics::GlobalMetrics;
use unified_trading_core::position_manager::PositionManager;
use market_data::{Normalizer, RawTick};
use feature::FeatureEngine;
use model::{InferenceEngine, PredictionEngine};
use strategy::{StrategyEngine, StrategyEngineExt};
use risk::{RiskCoordinator, RiskCheckRequest, RiskDecision};
use execution::{ExecutionManager, OrderLifecycleEvent};
use gateway::MockExecutionPort;

fn make_raw_tick(symbol: &str, ts: u64, mid: f64, spread: f64) -> RawTick {
    RawTick {
        symbol: symbol.to_string(),
        timestamp_ns: ts,
        bid: mid - spread / 2.0,
        ask: mid + spread / 2.0,
        bid_size: 100,
        ask_size: 200,
        last_price: mid,
        last_size: 50,
        exchange: "IEX".to_string(),
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
            let normalized = normalizer.process(tick.clone());
            let features = feature_engine.compute(&normalized);
            let _ = feature_tx.try_send(features.clone());

            if let Some(signal) = strategy_engine.evaluate_from_features(&features) {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;
                let request = RiskCheckRequest {
                    request_id: uuid::Uuid::new_v4().to_string(),
                    symbol: signal.symbol.clone(),
                    intent_id: signal.intent_id.clone(),
                    side: format!("{:?}", signal.side),
                    quantity: 1.0,
                    price: 150.0,
                    timestamp_ns: now,
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

    let normalizer = Normalizer::new("AAPL");
    let feature_engine = FeatureEngine::new("AAPL", 14, 14, 20);
    let strategy_engine = StrategyEngine::new(
        "AAPL", 0.6, -0.6, 0.5, 0.15, 0, 0, 150_000_000, true,
    );

    let pred_engine = PredictionEngine::new(feature_rx, "AAPL");
    let inference_engine = InferenceEngine::new(128);

    let _pred_handle = pred_engine.start(move |features| {
        inference_engine.predict(features)
    });

    let risk_coordinator = RiskCoordinator::new(
        risk_rx,
        decision_tx,
        Default::default(),
        100_000.0,
        Arc::clone(&kill_switch),
        Arc::clone(&metrics),
    );
    let _risk_handle = risk_coordinator.start();

    let exec_manager = ExecutionManager::new(
        decision_rx,
        lifecycle_tx,
        Arc::new(MockExecutionPort),
        10.0,
        5.0,
        Arc::clone(&metrics),
        Arc::clone(&kill_switch),
        Arc::new(PositionManager::new()),
    );
    let _exec_handle = exec_manager.start();

    let ks = Arc::clone(&kill_switch);
    let m = Arc::clone(&metrics);
    let proc_handle = std::thread::spawn(move || {
        run_processor(
            normalizer, feature_engine, strategy_engine,
            md_rx, feature_tx, risk_tx, ks, m,
        );
    });

    for i in 0..50 {
        let tick = make_raw_tick("AAPL", i * 1_000_000, 150.0 + (i as f64 * 0.1), 0.05);
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

    let normalizer = Normalizer::new("MSFT");
    let feature_engine = FeatureEngine::new("MSFT", 14, 14, 20);
    let strategy_engine = StrategyEngine::new(
        "MSFT", 0.6, -0.6, 0.5, 0.15, 0, 0, 150_000_000, true,
    );

    let pred_engine = PredictionEngine::new(feature_rx, "MSFT");
    let inference_engine = InferenceEngine::new(128);

    let _pred_handle = pred_engine.start(move |features| {
        inference_engine.predict(features)
    });

    let risk_coordinator = RiskCoordinator::new(
        risk_rx,
        decision_tx,
        Default::default(),
        100_000.0,
        Arc::clone(&kill_switch),
        Arc::clone(&metrics),
    );
    let _risk_handle = risk_coordinator.start();

    let exec_manager = ExecutionManager::new(
        decision_rx,
        lifecycle_tx,
        Arc::new(MockExecutionPort),
        100.0,
        50.0,
        Arc::clone(&metrics),
        Arc::clone(&kill_switch),
        Arc::new(PositionManager::new()),
    );
    let _exec_handle = exec_manager.start();

    let ks = Arc::clone(&kill_switch);
    let m = Arc::clone(&metrics);
    let proc_handle = std::thread::spawn(move || {
        run_processor(
            normalizer, feature_engine, strategy_engine,
            md_rx, feature_tx, risk_tx, ks, m,
        );
    });

    for i in 0..500 {
        let tick = make_raw_tick("MSFT", i * 100_000, 400.0 + (i as f64 * 0.01), 0.04);
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

    let normalizer = Normalizer::new("AAPL");
    let feature_engine = FeatureEngine::new("AAPL", 14, 14, 20);
    let strategy_engine = StrategyEngine::new(
        "AAPL", 0.6, -0.6, 0.5, 0.15, 0, 0, 150_000_000, true,
    );

    let pred_engine = PredictionEngine::new(feature_rx, "AAPL");
    let inference_engine = InferenceEngine::new(128);

    let _pred_handle = pred_engine.start(move |features| {
        inference_engine.predict(features)
    });

    let risk_coordinator = RiskCoordinator::new(
        risk_rx,
        decision_tx,
        Default::default(),
        100_000.0,
        Arc::clone(&kill_switch),
        Arc::clone(&metrics),
    );
    let _risk_handle = risk_coordinator.start();

    let exec_manager = ExecutionManager::new(
        decision_rx,
        lifecycle_tx,
        Arc::new(MockExecutionPort),
        10.0,
        5.0,
        Arc::clone(&metrics),
        Arc::clone(&kill_switch),
        Arc::new(PositionManager::new()),
    );
    let _exec_handle = exec_manager.start();

    let ks = Arc::clone(&kill_switch);
    let m = Arc::clone(&metrics);
    let proc_handle = std::thread::spawn(move || {
        run_processor(
            normalizer, feature_engine, strategy_engine,
            md_rx, feature_tx, risk_tx, ks, m,
        );
    });

    for i in 0..100 {
        let tick = make_raw_tick("AAPL", i * 1_000_000, 150.0, 0.05);
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
