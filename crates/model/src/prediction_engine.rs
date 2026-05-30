use arc_swap::ArcSwap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use feature::{FeatureVector, FeatureIndex};
use unified_trading_core::metrics::GlobalMetrics;
use unified_trading_core::symbol_registry::SymbolId;
use unified_trading_core::threading::{spawn_pinned, ThreadPriority};
use unified_trading_core::clock::wall_time_ns;

use crate::model_registry::ModelRegistry;

/// Model output produced by `PredictionEngine` and consumed by `Strategy`.
///
/// # Immutability Contract (Mandatory)
///
/// This struct is stored behind [`ArcSwap`](arc_swap::ArcSwap) in
/// `AssetProcessor.latest_pred` and hot-swapped on every inference tick.
/// **All fields must be plain data — no interior mutability.**
///
/// ## Forbidden types inside `Prediction`
/// - `Mutex<T>`, `RwLock<T>`, `RefCell<T>`, `Cell<T>` — these allow
///   in-place mutation that other threads could observe mid-update, causing
///   torn reads across fields during a hot-swap.
/// - Any heap-allocated mutable shared state (`Arc<Mutex<…>>`, etc.).
///
/// ## Allowed types
/// - `Copy` primitives (`f32`, `i32`, `u64`, …)
/// - Value-types that own their data (`String`, `Vec<T>` when T is immutable)
/// - `SymbolId`, other newtype wrappers around primitives
///
/// The compile-time lint in `crates/engine/tests/immutability_lint.rs`
/// will **fail to compile** if any forbidden type is added. Do not bypass it.
#[derive(Debug, Clone, PartialEq)]
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
    /// Monotonic version counter for ordering predictions.
    /// Used by AssetProcessor to reject stale predictions.
    pub version: u64,
    /// True when this prediction was produced by heuristic fallback.
    pub is_heuristic: bool,
}

impl Prediction {
    pub fn is_stale(&self, staleness_ns: u64) -> bool {
        let now = wall_time_ns();
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
            version: 0,
            is_heuristic: false,
        }
    }

    pub fn from_features(features: &FeatureVector, symbol_id: SymbolId) -> Self {
        let now = wall_time_ns();

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
            version: 0,
            is_heuristic: false,
        }
    }

    /// Create a heuristic prediction from raw features when the model is stale or unavailable.
    /// Uses MACD histogram as a simple trend-following signal with fixed confidence.
    /// All numeric fields are clamped to finite values to ensure the heuristic always
    /// produces a valid Prediction, even if the input features contain NaN/Inf.
    pub fn heuristic_from_features(features: &FeatureVector, symbol_id: SymbolId, version: u64) -> Self {
        let now = wall_time_ns();

        let macd_hist = Self::finify(features.get(FeatureIndex::MacdHistogram));
        let action_score = macd_hist;
        let confidence = 0.6f32; // Fixed heuristic confidence above typical minimums

        Self {
            symbol_id,
            forecast: Self::finify(features.get(FeatureIndex::MidPrice)),
            confidence,
            action_score,
            regime_label: features.get(FeatureIndex::Regime) as i32,
            regime_strength: Self::finify(features.get(FeatureIndex::RegimeStrength)),
            computed_ns: now,
            trace_id: features.trace_id,
            version,
            is_heuristic: true,
        }
    }

    /// Replace NaN/Inf with 0.0 so the heuristic always yields finite values.
    fn finify(v: f32) -> f32 {
        if v.is_finite() { v } else { 0.0 }
    }

    /// Apply a version and heuristic flag, returning a new Prediction.
    pub fn with_version(mut self, version: u64, is_heuristic: bool) -> Self {
        self.version = version;
        self.is_heuristic = is_heuristic;
        self
    }

    /// Returns true if all numeric fields are finite (not NaN, not Inf).
    pub fn is_valid(&self) -> bool {
        self.forecast.is_finite()
            && self.confidence.is_finite()
            && self.action_score.is_finite()
            && self.regime_strength.is_finite()
    }
}

/// Configuration for shadow model evaluation and promotion.
#[derive(Debug, Clone)]
pub struct ShadowConfig {
    /// Maximum allowed absolute forecast delta between active and shadow models
    pub forecast_delta_threshold: f32,
    /// Maximum allowed absolute confidence delta between active and shadow models
    pub confidence_delta_threshold: f32,
    /// Maximum allowed absolute action_score delta between active and shadow models
    pub action_score_delta_threshold: f32,
    /// Number of consecutive ticks where all deltas stay below thresholds before promoting
    pub promote_after_ticks: u64,
}

impl Default for ShadowConfig {
    fn default() -> Self {
        Self {
            forecast_delta_threshold: 0.1,
            confidence_delta_threshold: 0.1,
            action_score_delta_threshold: 0.1,
            promote_after_ticks: 1000,
        }
    }
}

/// Recorded divergence between active and shadow model predictions for a single tick.
#[derive(Debug, Clone)]
pub struct DivergenceMetrics {
    pub forecast_delta: f32,
    pub confidence_delta: f32,
    pub action_score_delta: f32,
}

impl DivergenceMetrics {
    pub fn compute(active: &Prediction, shadow: &Prediction) -> Self {
        Self {
            forecast_delta: (active.forecast - shadow.forecast).abs(),
            confidence_delta: (active.confidence - shadow.confidence).abs(),
            action_score_delta: (active.action_score - shadow.action_score).abs(),
        }
    }

    /// Returns the maximum of all delta values
    pub fn max_delta(&self) -> f32 {
        self.forecast_delta
            .max(self.confidence_delta)
            .max(self.action_score_delta)
    }

    /// Returns true if all deltas are below the given thresholds
    pub fn below_threshold(&self, config: &ShadowConfig) -> bool {
        self.forecast_delta <= config.forecast_delta_threshold
            && self.confidence_delta <= config.confidence_delta_threshold
            && self.action_score_delta <= config.action_score_delta_threshold
    }
}

pub struct PredictionEngine {
    pub feature_rx: crossbeam_channel::Receiver<FeatureVector>,
    pub latest_pred: Arc<ArcSwap<Prediction>>,
    pub symbol_id: SymbolId,
    running: Arc<AtomicBool>,
    /// Optional shadow model prediction output (not used for downstream decisions)
    pub shadow_pred: Arc<ArcSwap<Prediction>>,
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
            running: Arc::new(AtomicBool::new(true)),
            shadow_pred: Arc::new(ArcSwap::new(Arc::new(Prediction::new_default(symbol_id)))),
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
            shadow_pred: Arc::clone(&self.shadow_pred),
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

    /// Run loop that evaluates both active and shadow models on every FeatureVector.
    /// The active model's prediction is stored in `latest_pred` (used by downstream consumers).
    /// The shadow model's prediction is stored in `shadow_pred` (monitoring only).
    /// Divergence between the two is recorded in metrics.
    /// If the shadow model produces acceptable divergence for N consecutive ticks,
    /// it is automatically promoted to active via the registry.
    pub fn run_shadow_loop<F, G>(
        &self,
        mut active_infer_fn: F,
        mut shadow_infer_fn: G,
        shadow_config: ShadowConfig,
        metrics: &GlobalMetrics,
        registry: &ModelRegistry,
        shadow_model_id: &str,
    )
    where
        F: FnMut(&FeatureVector) -> Prediction + Send,
        G: FnMut(&FeatureVector) -> Prediction + Send,
    {
        let mut consecutive_good_ticks: u64 = 0;

        while self.running.load(Ordering::Relaxed) {
            match self.feature_rx.recv_timeout(std::time::Duration::from_millis(10)) {
                Ok(features) => {
                    let active_pred = active_infer_fn(&features);
                    let shadow_pred_val = shadow_infer_fn(&features);

                    let active_arc = Arc::new(active_pred);
                    let shadow_arc = Arc::new(shadow_pred_val);

                    // Always store active prediction for downstream use
                    self.latest_pred.store(Arc::clone(&active_arc));
                    // Store shadow prediction for monitoring
                    self.shadow_pred.store(Arc::clone(&shadow_arc));

                    // Compute and record divergence
                    let divergence = DivergenceMetrics::compute(&active_arc, &shadow_arc);
                    metrics.model_divergence.record(
                        (divergence.max_delta() * 1_000_000.0) as u64,
                    );

                    tracing::trace!(
                        symbol = ?self.symbol_id,
                        shadow_model = %shadow_model_id,
                        forecast_delta = %divergence.forecast_delta,
                        confidence_delta = %divergence.confidence_delta,
                        action_score_delta = %divergence.action_score_delta,
                        consecutive_good_ticks = %consecutive_good_ticks,
                        "Shadow model divergence"
                    );

                    if divergence.below_threshold(&shadow_config) {
                        consecutive_good_ticks += 1;
                        if consecutive_good_ticks >= shadow_config.promote_after_ticks {
                            tracing::info!(
                                symbol = ?self.symbol_id,
                                shadow_model = %shadow_model_id,
                                ticks = %consecutive_good_ticks,
                                "Shadow model divergence acceptable, promoting to active"
                            );
                            if let Err(e) = registry.promote_shadow() {
                                tracing::error!(
                                    symbol = ?self.symbol_id,
                                    shadow_model = %shadow_model_id,
                                    error = %e,
                                    "Failed to promote shadow model"
                                );
                            }
                            // Reset counter regardless of outcome to avoid repeated attempts
                            consecutive_good_ticks = 0;
                        }
                    } else {
                        consecutive_good_ticks = 0;
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    /// Start the shadow model inference thread.
    /// Returns a JoinHandle that must be joined for clean shutdown.
    pub fn start_shadow<F, G>(
        &self,
        active_infer_fn: F,
        shadow_infer_fn: G,
        shadow_config: ShadowConfig,
        metrics: Arc<GlobalMetrics>,
        registry: Arc<ModelRegistry>,
        shadow_model_id: String,
        core_id: usize,
    ) -> std::thread::JoinHandle<()>
    where
        F: FnMut(&FeatureVector) -> Prediction + Send + 'static,
        G: FnMut(&FeatureVector) -> Prediction + Send + 'static,
    {
        let engine = Self {
            feature_rx: self.feature_rx.clone(),
            latest_pred: Arc::clone(&self.latest_pred),
            shadow_pred: Arc::clone(&self.shadow_pred),
            symbol_id: self.symbol_id,
            running: Arc::clone(&self.running),
        };

        let symbol_id_val = self.symbol_id;
        spawn_pinned(
            &format!("shadow-{:?}", symbol_id_val),
            core_id,
            ThreadPriority::BelowNormal,
            move || {
                engine.run_shadow_loop(
                    active_infer_fn,
                    shadow_infer_fn,
                    shadow_config,
                    &*metrics,
                    &*registry,
                    &shadow_model_id,
                );
            },
        ).expect("spawn_pinned failed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::bounded;

    #[test]
    fn test_prediction_is_stale() {
        let pred = Prediction {
            symbol_id: SymbolId::from_raw(42),
            forecast: 0.0,
            confidence: 0.5,
            action_score: 0.0,
            regime_label: 0,
            regime_strength: 0.0,
            computed_ns: 100_000,
            trace_id: 1,
            version: 0,
            is_heuristic: false,
        };
        assert!(pred.is_stale(1000));
    }

    #[test]
    fn test_prediction_not_stale() {
        let now = unified_trading_core::clock::wall_time_ns();
        let pred = Prediction {
            symbol_id: SymbolId::from_raw(42),
            forecast: 0.0,
            confidence: 0.5,
            action_score: 0.0,
            regime_label: 0,
            regime_strength: 0.0,
            computed_ns: now - 10_000,
            trace_id: 1,
            version: 0,
            is_heuristic: false,
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
    fn test_divergence_metrics_compute() {
        let active = Prediction {
            symbol_id: SymbolId::from_raw(0),
            forecast: 0.5,
            confidence: 0.8,
            action_score: 0.3,
            regime_label: 0,
            regime_strength: 0.5,
            computed_ns: 1000,
            trace_id: 1,
            version: 0,
            is_heuristic: false,
        };
        let shadow = Prediction {
            symbol_id: SymbolId::from_raw(0),
            forecast: 0.7,
            confidence: 0.6,
            action_score: 0.4,
            regime_label: 0,
            regime_strength: 0.5,
            computed_ns: 1000,
            trace_id: 1,
            version: 0,
            is_heuristic: false,
        };
        let dm = DivergenceMetrics::compute(&active, &shadow);
        assert!((dm.forecast_delta - 0.2).abs() < 1e-6);
        assert!((dm.confidence_delta - 0.2).abs() < 1e-6);
        assert!((dm.action_score_delta - 0.1).abs() < 1e-6);
        assert!((dm.max_delta() - 0.2).abs() < 1e-6);
    }

    #[test]
    fn test_divergence_below_threshold() {
        let config = ShadowConfig {
            forecast_delta_threshold: 0.1,
            confidence_delta_threshold: 0.1,
            action_score_delta_threshold: 0.1,
            promote_after_ticks: 100,
        };
        let active = Prediction {
            symbol_id: SymbolId::from_raw(0),
            forecast: 0.5, confidence: 0.8, action_score: 0.3,
            regime_label: 0, regime_strength: 0.5, computed_ns: 1000, trace_id: 1,
            version: 0, is_heuristic: false,
        };
        let shadow = Prediction {
            symbol_id: SymbolId::from_raw(0),
            forecast: 0.45, confidence: 0.85, action_score: 0.32,
            regime_label: 0, regime_strength: 0.5, computed_ns: 1000, trace_id: 1,
            version: 0, is_heuristic: false,
        };
        let dm = DivergenceMetrics::compute(&active, &shadow);
        assert!(dm.below_threshold(&config));

        let divergent_shadow = Prediction {
            symbol_id: SymbolId::from_raw(0),
            forecast: 0.9, confidence: 0.3, action_score: 0.9,
            regime_label: 0, regime_strength: 0.5, computed_ns: 1000, trace_id: 1,
            version: 0, is_heuristic: false,
        };
        let dm2 = DivergenceMetrics::compute(&active, &divergent_shadow);
        assert!(!dm2.below_threshold(&config));
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
                computed_ns: unified_trading_core::clock::wall_time_ns(),
                trace_id: features.trace_id,
                version: 0,
                is_heuristic: false,
            }
        }, 0);

        std::thread::sleep(std::time::Duration::from_millis(50));
        engine.stop();
        let _ = handle.join();

        let pred = engine.get_prediction();
        assert!((pred.forecast - 150.0).abs() < 0.01);
        assert_eq!(pred.trace_id, 42);
    }

    #[test]
    fn test_prediction_is_valid_rejects_nan() {
        let valid_pred = Prediction {
            symbol_id: SymbolId::from_raw(1),
            forecast: 0.5, confidence: 0.8, action_score: 0.3,
            regime_label: 0, regime_strength: 0.5,
            computed_ns: 1000, trace_id: 1,
            version: 0, is_heuristic: false,
        };
        assert!(valid_pred.is_valid());

        let nan_pred = Prediction {
            forecast: f32::NAN, confidence: 0.8, action_score: 0.3,
            ..valid_pred
        };
        assert!(!nan_pred.is_valid());

        let inf_pred = Prediction {
            forecast: 0.5, confidence: f32::INFINITY, action_score: 0.3,
            ..valid_pred
        };
        assert!(!inf_pred.is_valid());
    }

    #[test]
    fn test_heuristic_from_features_sets_is_heuristic_and_version() {
        let mut fv = FeatureVector::new(SymbolId::from_raw(1), 1000, 42);
        fv.set(FeatureIndex::MidPrice, 150.0);
        fv.set(FeatureIndex::MacdHistogram, 0.3);
        fv.set(FeatureIndex::Regime, 1.0);
        fv.set(FeatureIndex::RegimeStrength, 0.6);

        let pred = Prediction::heuristic_from_features(&fv, SymbolId::from_raw(1), 7);

        assert!(pred.is_heuristic, "heuristic_from_features must set is_heuristic=true");
        assert_eq!(pred.version, 7, "version must match the provided version");
        assert!(pred.is_valid(), "heuristic prediction must be valid");
        assert_eq!(pred.trace_id, 42, "trace_id must be propagated");
    }

    #[test]
    fn test_with_version_preserves_fields() {
        let pred = Prediction::new_default(SymbolId::from_raw(1));
        let updated = Prediction::with_version(pred, 5, true);

        assert_eq!(updated.version, 5);
        assert!(updated.is_heuristic);
        assert_eq!(updated.symbol_id, SymbolId::from_raw(1));
    }
}
