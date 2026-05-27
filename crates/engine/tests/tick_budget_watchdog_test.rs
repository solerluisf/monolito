use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use crossbeam_channel::bounded;

use unified_trading_engine::engine::{AssetProcessor, StrategySwapRef};
use feature::{FeatureEngine, FeatureVector};
use market_data::{Normalizer, RawTick};
use model::Prediction;
use risk::RiskCheckRequest;
use strategy::{SignalContext, Strategy, TradeIntent};
use unified_trading_core::config::BackpressurePolicy;
use unified_trading_core::heartbeat::ThreadHeartbeatMonitor;
use unified_trading_core::kill_switch::KillSwitch;
use unified_trading_core::metrics::GlobalMetrics;
use unified_trading_core::symbol_registry::SymbolId;

#[derive(Clone)]
struct SlowNoopStrategy {
    sleep_ms: u64,
}

impl Strategy for SlowNoopStrategy {
    fn name(&self) -> &str {
        "slow-noop"
    }

    fn evaluate(
        &self,
        _prediction: &Prediction,
        _ctx: &SignalContext,
    ) -> Option<TradeIntent> {
        std::thread::sleep(Duration::from_millis(self.sleep_ms));
        None
    }

    fn clone_box(&self) -> Box<dyn Strategy> {
        Box::new(self.clone())
    }
}

fn make_tick(symbol_id: SymbolId, ts: u64) -> RawTick {
    RawTick {
        symbol_id,
        timestamp_ns: ts,
        bid: 100.0,
        ask: 100.1,
        bid_size: 10,
        ask_size: 10,
        last_price: 100.05,
        last_size: 1,
        exchange: "TEST".to_string(),
    }
}

#[test]
fn test_watchdog_triggers_and_batch_ticks_are_skipped() {
    let kill_switch = Arc::new(KillSwitch::new());
    let metrics = Arc::new(GlobalMetrics::new());

    let hb_monitor = ThreadHeartbeatMonitor::new(
        Arc::clone(&kill_switch),
        Arc::clone(&metrics),
        20_000_000, // 20ms global timeout
        2,          // check every 2ms
        0,
    );

    let symbol_id = SymbolId::from_raw(7);
    let hb = hb_monitor.register_thread("asset-SYNTH");
    hb.pulse();

    let strategy: StrategySwapRef = Arc::new(ArcSwap::new(Arc::new(
        Box::new(SlowNoopStrategy { sleep_ms: 10 }) as Box<dyn strategy::Strategy>
    )));

    let prediction = Arc::new(Prediction {
        symbol_id,
        forecast: 0.0,
        confidence: 1.0,
        action_score: 0.0,
        regime_label: 0,
        regime_strength: 0.0,
        computed_ns: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64,
    });
    let latest_pred = Arc::new(ArcSwap::new(prediction));

    let (md_tx, md_rx) = bounded::<RawTick>(16);
    let (feature_tx, feature_rx) = bounded::<FeatureVector>(16);
    let (risk_tx, risk_rx) = bounded::<RiskCheckRequest>(16);

    for i in 0..4_u64 {
        md_tx.send(make_tick(symbol_id, i * 1_000_000)).expect("tick send");
    }

    let mut processor = AssetProcessor {
        symbol: "SYNTH".to_string(),
        symbol_id,
        normalizer: Normalizer::new(symbol_id),
        feature_engine: FeatureEngine::new(
            "SYNTH", 14, 14, 9, 20, 50, 20, 20, 1, 5, 20, 0.3, 0.02, 0.05, 0.5
        ),
        strategy,
        latest_pred,
        signal_ctx: SignalContext::new(symbol_id),
        coordinator_tx: risk_tx,
        coordinator_rx: risk_rx,
        feature_tx,
        feature_rx,
        feature_backpressure_policy: BackpressurePolicy::DropNewest,
        risk_backpressure_policy: BackpressurePolicy::DropNewest,
        kill_switch: Arc::clone(&kill_switch),
        metrics: Arc::clone(&metrics),
        prediction_staleness_ns: 1_000_000_000,
        default_order_quantity: 1.0,
        tick_processing_budget_us: 500,
        heartbeat: Some(hb),
    };

    let budget_breaches = Arc::new(AtomicUsize::new(0));
    let skipped_ticks = Arc::new(AtomicUsize::new(0));
    let budget_breaches_cb = Arc::clone(&budget_breaches);
    let skipped_ticks_cb = Arc::clone(&skipped_ticks);

    processor.run_loop_with_options(
        &md_rx,
        4,
        Some(&move |_elapsed_us, _budget_us, skipped| {
            budget_breaches_cb.fetch_add(1, Ordering::Relaxed);
            skipped_ticks_cb.fetch_add(skipped, Ordering::Relaxed);
        }),
    );

    std::thread::sleep(Duration::from_millis(30));

    assert!(kill_switch.is_active(), "watchdog should trigger kill-switch");
    assert!(
        budget_breaches.load(Ordering::Relaxed) >= 1,
        "expected at least one tick budget breach"
    );
    assert!(
        skipped_ticks.load(Ordering::Relaxed) >= 1,
        "expected at least one skipped tick from batch remainder"
    );

    let mut hb_monitor = hb_monitor;
    hb_monitor.shutdown();
}
