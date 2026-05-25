use crate::strategy_engine::{TradeIntent, SignalSide, SizeHint, IntentType, StrategyEngine};
use feature::FeatureVector;

pub trait StrategyEngineExt {
    fn evaluate_from_features(&mut self, features: &FeatureVector) -> Option<TradeIntent>;
    fn compute_action_score(&self, rsi: f32, macd_hist: f32, atr: f32, volume_ratio: f32) -> f32;
    fn compute_confidence(&self, rsi: f32, macd_hist: f32, regime_strength: f32) -> f32;
}

impl StrategyEngineExt for StrategyEngine {
    #[tracing::instrument(skip_all, fields(symbol = %features.symbol))]
    fn evaluate_from_features(&mut self, features: &FeatureVector) -> Option<TradeIntent> {
        let rsi = features.get("rsi_14").unwrap_or(50.0);
        let macd_hist = features.get("macd_histogram").unwrap_or(0.0);
        let atr = features.get("atr_14").unwrap_or(0.0);
        let volume_ratio = features.get("volume_ratio").unwrap_or(1.0);
        let regime_strength = features.get("regime_strength").unwrap_or(0.0);

        let action_score = self.compute_action_score(rsi, macd_hist, atr, volume_ratio);
        let confidence = self.compute_confidence(rsi, macd_hist, regime_strength);

        if confidence < self.confidence_minimum as f32 {
            return None;
        }

        if action_score > self.long_entry_threshold as f32 {
            Some(TradeIntent::new(
                &self.symbol,
                SignalSide::Long,
                SizeHint::Units(1),
                IntentType::Entry,
                confidence as f64,
                action_score as f64,
            ))
        } else if action_score < self.short_entry_threshold as f32 {
            Some(TradeIntent::new(
                &self.symbol,
                SignalSide::Short,
                SizeHint::Units(1),
                IntentType::Entry,
                confidence as f64,
                action_score as f64,
            ))
        } else {
            None
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

    fn compute_confidence(&self, rsi: f32, macd_hist: f32, regime_strength: f32) -> f32 {
        let rsi_conf = 1.0 - ((rsi - 50.0) / 50.0).abs();
        let macd_conf = macd_hist.abs().min(1.0);
        let regime_conf = regime_strength;

        (rsi_conf * 0.3 + macd_conf * 0.4 + regime_conf * 0.3).clamp(0.0, 1.0)
    }
}
