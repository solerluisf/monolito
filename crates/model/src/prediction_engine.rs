use arc_swap::ArcSwap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use feature::{FeatureVector, FeatureIndex};
use unified_trading_core::symbol_registry::SymbolId;
use unified_trading_core::threading::{spawn_pinned, ThreadPriority};

#[derive(Debug, Clone)]
pub struct Prediction {
    pub symbol_id: SymbolId,
    pub forecast: f32,
    pub confidence: f32,
    pub action_score: f32,
    pub regime_label: i32,
    pub regime_strength: f32,
    pub computed_ns: u64,
    /// Trace ID propagated from RawTick for causal tracing.
    pub trace_id: u64,
}

impl Prediction {
    pub fn is_stale(&self, staleness_ns: u64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        now.saturating_sub(self.computed_ns) > staleness_ns
    }

    pub fn new_default(symbol_id: SymbolId) -> Self {
        Self {
            symbol_id,
            forecast: 0.0,
            confidence: 0.0,
            action_score: 0.0,
            regime_label: 0,
            regime_strength: 0.0,
            computed_ns: 0,
            trace_id: 0,
        }
    }

    pub fn from_features(features: &FeatureVector, symbol_id: SymbolId) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let forecast = features.get(FeatureIndex::MidPrice);
        let confidence = features.get(FeatureIndex::Confidence);
        let action_score = features.get(FeatureIndex::MacdHistogram);
        let regime_label = features.get(FeatureIndex::Regime) as i32;
        let regime_strength = features.get(FeatureIndex::RegimeStrength);

        Self {
            symbol_id,
            forecast,
            confidence,
            action_score,
            regime_label,
            regime_strength,
            computed_ns: now,
            trace_id: features.trace_id,
        }
    }

    /// Create a heuristic prediction from raw features when the model is stale or unavailable.
    /// Uses MACD histogram as a simple trend-following signal with fixed confidence.
    pub fn heuristic_from_features(features: &FeatureVector, symbol_id: SymbolId) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let macd_hist = features.get(FeatureIndex::MacdHistogram);
        let action_score = macd_hist;
        let confidence = 0.6f32; // Fixed heuristic confidence above typical minimums

        Self {
            symbol_id,
            forecast: features.get(FeatureIndex::MidPrice),
            confidence,
            action_score,
            regime_label: features.get(FeatureIndex::Regime) as i32,
            regime_strength: features.get(FeatureIndex::RegimeStrength),
            computed_ns: now,
            trace_id: features.trace_id,
        }
    }
}

pub struct PredictionEngine {
    pub feature_rx: crossbeam_channel::Receiver<FeatureVector>,
    pub latest_pred: Arc<ArcSwap<Prediction>>,
    pub symbol_id: SymbolId,
    running: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl PredictionEngine {
    pub fn new(
        feature_rx: crossbeam_channel::Receiver<FeatureVector>,
        symbol_id: SymbolId,
    ) -> Self {
        Self {
            feature_rx,
            latest_pred: Arc::new(ArcSwap::new(Arc::new(Prediction::new_default(symbol_id)))),
            symbol_id,
            running: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
        }
    }

    pub fn run_loop<F>(&self, mut infer_fn: F)
    where
        F: FnMut(&FeatureVector) -> Prediction + Send,
    {
        while self.running.load(std::sync::atomic::Ordering::Relaxed) {
            match self.feature_rx.recv_timeout(std::time::Duration::from_millis(10)) {
                Ok(features) => {
                    let pred = infer_fn(&features);
                    self.latest_pred.store(Arc::new(pred));
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    pub fn start<F>(&self, infer_fn: F, core_id: usize) -> std::thread::JoinHandle<()>
    where
        F: FnMut(&FeatureVector) -> Prediction + Send + 'static,
    {
        let engine = Self {
            feature_rx: self.feature_rx.clone(),
            latest_pred: Arc::clone(&self.latest_pred),
            symbol_id: self.symbol_id,
            running: Arc::clone(&self.running),
        };

        let symbol_id_val = self.symbol_id;
        spawn_pinned(
            &format!("prediction-{:?}", symbol_id_val),
            core_id,
            ThreadPriority::BelowNormal,
            move || {
                engine.run_loop(infer_fn);
            },
        ).expect("spawn_pinned failed")
    }

    pub fn stop(&self) {
        self.running.store(false, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn get_prediction(&self) -> Arc<Prediction> {
        Arc::clone(&self.latest_pred.load())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::bounded;

    #[test]
    fn test_prediction_is_stale() {
        let pred = Prediction {
            symbol_id: SymbolId::from_raw(0),
            forecast: 0.5,
            confidence: 0.8,
            action_score: 0.3,
            regime_label: 0,
            regime_strength: 0.5,
            computed_ns: 0,
            trace_id: 1,
        };
        assert!(pred.is_stale(1000));
    }

    #[test]
    fn test_prediction_not_stale() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let pred = Prediction {
            symbol_id: SymbolId::from_raw(0),
            forecast: 0.5,
            confidence: 0.8,
            action_score: 0.3,
            regime_label: 0,
            regime_strength: 0.5,
            computed_ns: now,
            trace_id: 2,
        };
        assert!(!pred.is_stale(1_000_000_000));
    }

    #[test]
    fn test_prediction_engine_default() {
        let (tx, rx) = bounded::<FeatureVector>(100);
        let symbol_id = SymbolId::from_raw(0);
        let engine = PredictionEngine::new(rx, symbol_id);
        let pred = engine.get_prediction();
        assert_eq!(pred.symbol_id, symbol_id);
        assert_eq!(pred.forecast, 0.0);
        drop(tx);
    }

    #[test]
    fn test_prediction_engine_receives_features() {
        let (tx, rx) = bounded::<FeatureVector>(100);
        let symbol_id = SymbolId::from_raw(0);
        let engine = PredictionEngine::new(rx, symbol_id);

        let mut fv = FeatureVector::new(symbol_id, 1000, 42);
        fv.set(FeatureIndex::MidPrice, 150.0);
        tx.send(fv).unwrap();

        let handle = engine.start(|features| {
            let mid = features.get(FeatureIndex::MidPrice);
            Prediction {
                symbol_id: features.symbol_id,
                forecast: mid as f32,
                confidence: 0.8,
                action_score: 0.5,
                regime_label: 0,
                regime_strength: 0.5,
                computed_ns: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64,
                trace_id: features.trace_id,
            }
        }, 0);

        std::thread::sleep(std::time::Duration::from_millis(50));
        engine.stop();
        let _ = handle.join();

        let pred = engine.get_prediction();
        assert!((pred.forecast - 150.0).abs() < 0.01);
        assert_eq!(pred.trace_id, 42);
    }
}
