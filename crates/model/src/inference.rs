use feature::FeatureVector;
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

        let mid_price = features.get("mid_price").unwrap_or(0.0) as f32;
        let rsi = features.get("rsi_14").unwrap_or(self.rsi_neutral as f32);
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
        let engine = make_engine();
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
        let engine = make_engine();
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
        let engine = make_engine();
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
        let engine = make_engine();
        let fv = FeatureVector::new("AAPL", 1000, 20);
        let pred = engine.predict(&fv);
        assert!(pred.forecast >= -1.0 && pred.forecast <= 1.0);
    }
}
