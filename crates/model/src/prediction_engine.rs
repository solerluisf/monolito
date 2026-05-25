use arc_swap::ArcSwap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use feature::FeatureVector;
use unified_trading_core::threading::{spawn_pinned, ThreadPriority};

#[derive(Debug, Clone)]
pub struct Prediction {
    pub symbol: String,
    pub forecast: f32,
    pub confidence: f32,
    pub action_score: f32,
    pub regime_label: i32,
    pub regime_strength: f32,
    pub computed_ns: u64,
}

impl Prediction {
    pub fn is_stale(&self, staleness_ns: u64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        now.saturating_sub(self.computed_ns) > staleness_ns
    }

    pub fn new_default(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
            forecast: 0.0,
            confidence: 0.0,
            action_score: 0.0,
            regime_label: 0,
            regime_strength: 0.0,
            computed_ns: 0,
        }
    }

    pub fn from_features(features: &FeatureVector, symbol: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let forecast = features.get("forecast").unwrap_or(0.0) as f32;
        let confidence = features.get("confidence").unwrap_or(0.0) as f32;
        let action_score = features.get("action_score").unwrap_or(0.0) as f32;
        let regime_label = features.get("regime").unwrap_or(0.0) as i32;
        let regime_strength = features.get("regime_strength").unwrap_or(0.0) as f32;

        Self {
            symbol: symbol.to_string(),
            forecast,
            confidence,
            action_score,
            regime_label,
            regime_strength,
            computed_ns: now,
        }
    }
}

pub struct PredictionEngine {
    pub feature_rx: crossbeam_channel::Receiver<FeatureVector>,
    pub latest_pred: Arc<ArcSwap<Prediction>>,
    pub symbol: String,
    running: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl PredictionEngine {
    pub fn new(
        feature_rx: crossbeam_channel::Receiver<FeatureVector>,
        symbol: &str,
    ) -> Self {
        Self {
            feature_rx,
            latest_pred: Arc::new(ArcSwap::new(Arc::new(Prediction::new_default(symbol)))),
            symbol: symbol.to_string(),
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
            symbol: self.symbol.clone(),
            running: Arc::clone(&self.running),
        };

        spawn_pinned(
            &format!("prediction-{}", self.symbol),
            core_id,
            ThreadPriority::BelowNormal,
            move || {
                engine.run_loop(infer_fn);
            },
        )
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
            symbol: "AAPL".to_string(),
            forecast: 0.5,
            confidence: 0.8,
            action_score: 0.3,
            regime_label: 0,
            regime_strength: 0.5,
            computed_ns: 0,
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
            symbol: "AAPL".to_string(),
            forecast: 0.5,
            confidence: 0.8,
            action_score: 0.3,
            regime_label: 0,
            regime_strength: 0.5,
            computed_ns: now,
        };
        assert!(!pred.is_stale(1_000_000_000));
    }

    #[test]
    fn test_prediction_engine_default() {
        let (tx, rx) = bounded::<FeatureVector>(100);
        let engine = PredictionEngine::new(rx, "AAPL");
        let pred = engine.get_prediction();
        assert_eq!(pred.symbol, "AAPL");
        assert_eq!(pred.forecast, 0.0);
        drop(tx);
    }

    #[test]
    fn test_prediction_engine_receives_features() {
        let (tx, rx) = bounded::<FeatureVector>(100);
        let engine = PredictionEngine::new(rx, "AAPL");

        let mut fv = FeatureVector::new("AAPL", 1000, 10);
        fv.push("mid_price", 150.0);
        tx.send(fv).unwrap();

        let handle = engine.start(|features| {
            let mid = features.get("mid_price").unwrap_or(0.0);
            Prediction {
                symbol: features.symbol.clone(),
                forecast: mid as f32,
                confidence: 0.8,
                action_score: 0.5,
                regime_label: 0,
                regime_strength: 0.5,
                computed_ns: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64,
            }
        }, 0);

        std::thread::sleep(std::time::Duration::from_millis(50));
        engine.stop();
        let _ = handle.join();

        let pred = engine.get_prediction();
        assert!((pred.forecast - 150.0).abs() < 0.01);
    }
}
