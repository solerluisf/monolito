use feature::FeatureVector;
use crate::prediction_engine::Prediction;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct InferenceEngine {
    pub feature_vector_size: usize,
}

impl InferenceEngine {
    pub fn new(feature_vector_size: usize) -> Self {
        Self {
            feature_vector_size,
        }
    }

    pub fn predict(&self, features: &FeatureVector) -> Prediction {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let mid_price = features.get("mid_price").unwrap_or(0.0) as f32;
        let rsi = features.get("rsi_14").unwrap_or(50.0);
        let macd_hist = features.get("macd_histogram").unwrap_or(0.0);
        let atr = features.get("atr_14").unwrap_or(0.0);
        let volume_ratio = features.get("volume_ratio").unwrap_or(1.0);
        let regime = features.get("regime").unwrap_or(0.0);
        let regime_strength = features.get("regime_strength").unwrap_or(0.0);
        let confidence = features.get("confidence").unwrap_or(0.5);

        let action_score = self.compute_action_score(rsi, macd_hist, atr, volume_ratio);
        let forecast = self.compute_forecast(macd_hist, rsi, volume_ratio);

        Prediction {
            symbol: features.symbol.clone(),
            forecast,
            confidence: confidence,
            action_score,
            regime_label: regime as i32,
            regime_strength,
            computed_ns: now,
        }
    }

    fn compute_action_score(&self, rsi: f32, macd_hist: f32, atr: f32, volume_ratio: f32) -> f32 {
        let rsi_component = if rsi > 70.0 {
            -0.5
        } else if rsi < 30.0 {
            0.5
        } else {
            (50.0 - rsi) / 50.0
        };

        let macd_component = macd_hist.signum() * macd_hist.abs().min(1.0);
        let vol_component = (volume_ratio - 1.0).clamp(-0.3, 0.3);
        let atr_penalty = if atr > 2.0 { -0.2 } else { 0.0 };

        (rsi_component * 0.4 + macd_component * 0.4 + vol_component * 0.2 + atr_penalty)
            .clamp(-1.0, 1.0)
    }

    fn compute_forecast(&self, macd_hist: f32, rsi: f32, volume_ratio: f32) -> f32 {
        let trend = macd_hist.signum() * macd_hist.abs().sqrt().min(0.5);
        let momentum = if rsi > 50.0 { 0.2 } else { -0.2 };
        let vol_confirmation = if volume_ratio > 1.2 { 0.1 } else { -0.1 };

        (trend + momentum * 0.3 + vol_confirmation * 0.2).clamp(-1.0, 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_features() -> FeatureVector {
        let mut fv = FeatureVector::new("AAPL", 1000, 20);
        fv.push("mid_price", 150.0);
        fv.push("rsi_14", 55.0);
        fv.push("macd_histogram", 0.3);
        fv.push("atr_14", 0.5);
        fv.push("volume_ratio", 1.2);
        fv.push("regime", 1.0);
        fv.push("regime_strength", 0.6);
        fv.push("confidence", 0.7);
        fv
    }

    #[test]
    fn test_inference_engine_predict() {
        let engine = InferenceEngine::new(128);
        let features = make_features();
        let pred = engine.predict(&features);

        assert_eq!(pred.symbol, "AAPL");
        assert!(pred.forecast >= -1.0 && pred.forecast <= 1.0);
        assert!(pred.action_score >= -1.0 && pred.action_score <= 1.0);
        assert!(pred.confidence > 0.0);
        assert!(pred.computed_ns > 0);
    }

    #[test]
    fn test_inference_engine_bullish_signal() {
        let engine = InferenceEngine::new(128);
        let mut fv = FeatureVector::new("AAPL", 1000, 20);
        fv.push("mid_price", 150.0);
        fv.push("rsi_14", 25.0);
        fv.push("macd_histogram", 0.5);
        fv.push("atr_14", 0.3);
        fv.push("volume_ratio", 1.5);
        fv.push("regime", 1.0);
        fv.push("regime_strength", 0.8);
        fv.push("confidence", 0.9);

        let pred = engine.predict(&fv);
        assert!(pred.action_score > 0.0);
    }

    #[test]
    fn test_inference_engine_bearish_signal() {
        let engine = InferenceEngine::new(128);
        let mut fv = FeatureVector::new("AAPL", 1000, 20);
        fv.push("mid_price", 150.0);
        fv.push("rsi_14", 75.0);
        fv.push("macd_histogram", -0.5);
        fv.push("atr_14", 0.3);
        fv.push("volume_ratio", 1.5);
        fv.push("regime", 0.0);
        fv.push("regime_strength", 0.3);
        fv.push("confidence", 0.8);

        let pred = engine.predict(&fv);
        assert!(pred.action_score < 0.0);
    }

    #[test]
    fn test_inference_engine_missing_features() {
        let engine = InferenceEngine::new(128);
        let fv = FeatureVector::new("AAPL", 1000, 20);
        let pred = engine.predict(&fv);
        assert!(pred.forecast >= -1.0 && pred.forecast <= 1.0);
    }
}
