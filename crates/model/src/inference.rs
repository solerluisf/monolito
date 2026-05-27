use feature::{FeatureVector, FeatureIndex};
use crate::prediction_engine::Prediction;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct InferenceEngine {
    pub feature_vector_size: usize,
    pub action_score_rsi_weight: f64,
    pub action_score_macd_weight: f64,
    pub action_score_volatility_weight: f64,
    pub atr_penalty_threshold: f64,
    pub atr_penalty_value: f64,
    pub rsi_overbought: f64,
    pub rsi_oversold: f64,
    pub rsi_neutral: f64,
    pub forecast_momentum_weight: f64,
    pub forecast_volume_weight: f64,
    pub volume_ratio_clamp: f64,
    pub volume_confirmation_threshold: f64,
}

impl InferenceEngine {
    pub fn new(
        feature_vector_size: usize,
        action_score_rsi_weight: f64,
        action_score_macd_weight: f64,
        action_score_volatility_weight: f64,
        atr_penalty_threshold: f64,
        atr_penalty_value: f64,
        rsi_overbought: f64,
        rsi_oversold: f64,
        rsi_neutral: f64,
        forecast_momentum_weight: f64,
        forecast_volume_weight: f64,
        volume_ratio_clamp: f64,
        volume_confirmation_threshold: f64,
    ) -> Self {
        Self {
            feature_vector_size,
            action_score_rsi_weight,
            action_score_macd_weight,
            action_score_volatility_weight,
            atr_penalty_threshold,
            atr_penalty_value,
            rsi_overbought,
            rsi_oversold,
            rsi_neutral,
            forecast_momentum_weight,
            forecast_volume_weight,
            volume_ratio_clamp,
            volume_confirmation_threshold,
        }
    }

    pub fn predict(&self, features: &FeatureVector) -> Prediction {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let mid_price = features.get(FeatureIndex::MidPrice);
        let rsi = features.get(FeatureIndex::Rsi14);
        let macd_hist = features.get(FeatureIndex::MacdHistogram);
        let atr = features.get(FeatureIndex::Atr14);
        let volume_ratio = features.get(FeatureIndex::VolumeRatio);
        let regime = features.get(FeatureIndex::Regime);
        let regime_strength = features.get(FeatureIndex::RegimeStrength);
        let confidence = features.get(FeatureIndex::Confidence);

        let action_score = self.compute_action_score(rsi, macd_hist, atr, volume_ratio);
        let forecast = self.compute_forecast(macd_hist, rsi, volume_ratio);

        Prediction {
            symbol_id: features.symbol_id,
            forecast,
            confidence: confidence,
            action_score,
            regime_label: regime as i32,
            regime_strength,
            computed_ns: now,
        }
    }

    fn compute_action_score(&self, rsi: f32, macd_hist: f32, atr: f32, volume_ratio: f32) -> f32 {
        let rsi_component = if rsi > self.rsi_overbought as f32 {
            -0.5
        } else if rsi < self.rsi_oversold as f32 {
            0.5
        } else {
            (self.rsi_neutral as f32 - rsi) / self.rsi_neutral as f32
        };

        let macd_component = macd_hist.signum() * macd_hist.abs().min(1.0);
        let clamp = self.volume_ratio_clamp as f32;
        let vol_component = (volume_ratio - 1.0).clamp(-clamp, clamp);
        let atr_penalty = if atr > self.atr_penalty_threshold as f32 {
            self.atr_penalty_value as f32
        } else {
            0.0
        };

        (rsi_component * self.action_score_rsi_weight as f32
            + macd_component * self.action_score_macd_weight as f32
            + vol_component * self.action_score_volatility_weight as f32
            + atr_penalty)
            .clamp(-1.0, 1.0)
    }

    fn compute_forecast(&self, macd_hist: f32, rsi: f32, volume_ratio: f32) -> f32 {
        let trend = macd_hist.signum() * macd_hist.abs().sqrt().min(0.5);
        let momentum = if rsi > self.rsi_neutral as f32 { 0.2 } else { -0.2 };
        let vol_confirmation = if volume_ratio > self.volume_confirmation_threshold as f32 {
            0.1
        } else {
            -0.1
        };

        (trend + momentum * self.forecast_momentum_weight as f32
            + vol_confirmation * self.forecast_volume_weight as f32)
            .clamp(-1.0, 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

use unified_trading_core::symbol_registry::SymbolId;

    fn make_engine() -> InferenceEngine {
        InferenceEngine::new(
            128,
            0.4, 0.4, 0.2,
            2.0, -0.2,
            70.0, 30.0, 50.0,
            0.3, 0.2,
            0.3, 1.2,
        )
    }

    fn make_features() -> FeatureVector {
        let mut fv = FeatureVector::new(SymbolId::from_raw(0), 1000);
        fv.set(FeatureIndex::MidPrice, 150.0);
        fv.set(FeatureIndex::Rsi14, 55.0);
        fv.set(FeatureIndex::MacdHistogram, 0.3);
        fv.set(FeatureIndex::Atr14, 0.5);
        fv.set(FeatureIndex::VolumeRatio, 1.2);
        fv.set(FeatureIndex::Regime, 1.0);
        fv.set(FeatureIndex::RegimeStrength, 0.6);
        fv.set(FeatureIndex::Confidence, 0.7);
        fv
    }

    #[test]
    fn test_inference_engine_predict() {
        let engine = make_engine();
        let features = make_features();
        let pred = engine.predict(&features);

        assert_eq!(pred.symbol_id, SymbolId::from_raw(0));
        assert!(pred.forecast >= -1.0 && pred.forecast <= 1.0);
        assert!(pred.action_score >= -1.0 && pred.action_score <= 1.0);
        assert!(pred.confidence > 0.0);
        assert!(pred.computed_ns > 0);
    }

    #[test]
    fn test_inference_engine_bullish_signal() {
        let engine = make_engine();
        let mut fv = FeatureVector::new(SymbolId::from_raw(0), 1000);
        fv.set(FeatureIndex::MidPrice, 150.0);
        fv.set(FeatureIndex::Rsi14, 25.0);
        fv.set(FeatureIndex::MacdHistogram, 0.5);
        fv.set(FeatureIndex::Atr14, 0.3);
        fv.set(FeatureIndex::VolumeRatio, 1.5);
        fv.set(FeatureIndex::Regime, 1.0);
        fv.set(FeatureIndex::RegimeStrength, 0.8);
        fv.set(FeatureIndex::Confidence, 0.9);

        let pred = engine.predict(&fv);
        assert!(pred.action_score > 0.0);
    }

    #[test]
    fn test_inference_engine_bearish_signal() {
        let engine = make_engine();
        let mut fv = FeatureVector::new(SymbolId::from_raw(0), 1000);
        fv.set(FeatureIndex::MidPrice, 150.0);
        fv.set(FeatureIndex::Rsi14, 75.0);
        fv.set(FeatureIndex::MacdHistogram, -0.5);
        fv.set(FeatureIndex::Atr14, 0.3);
        fv.set(FeatureIndex::VolumeRatio, 1.5);
        fv.set(FeatureIndex::Regime, 0.0);
        fv.set(FeatureIndex::RegimeStrength, 0.3);
        fv.set(FeatureIndex::Confidence, 0.8);

        let pred = engine.predict(&fv);
        assert!(pred.action_score < 0.0);
    }

    #[test]
    fn test_inference_engine_missing_features() {
        let engine = make_engine();
        let fv = FeatureVector::new(SymbolId::from_raw(0), 1000);
        let pred = engine.predict(&fv);
        assert!(pred.forecast >= -1.0 && pred.forecast <= 1.0);
    }
}
