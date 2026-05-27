use crate::{TradeIntent, SignalSide, SizeHint, IntentType};
use crate::strategy_engine::StrategyEngine;
use feature::{FeatureVector, FeatureIndex};

pub trait StrategyEngineExt {
    fn evaluate_from_features(&mut self, features: &FeatureVector) -> Option<TradeIntent>;
    fn compute_action_score(&self, rsi: f32, macd_hist: f32, atr: f32, volume_ratio: f32) -> f32;
    fn compute_confidence(&self, rsi: f32, macd_hist: f32, regime_strength: f32) -> f32;
}

impl StrategyEngineExt for StrategyEngine {
    #[tracing::instrument(skip_all, fields(symbol_id = %features.symbol_id))]
    fn evaluate_from_features(&mut self, features: &FeatureVector) -> Option<TradeIntent> {
        let rsi = features.get(FeatureIndex::Rsi14);
        let macd_hist = features.get(FeatureIndex::MacdHistogram);
        let atr = features.get(FeatureIndex::Atr14);
        let volume_ratio = features.get(FeatureIndex::VolumeRatio);
        let regime_strength = features.get(FeatureIndex::RegimeStrength);

        let action_score = self.compute_action_score(rsi, macd_hist, atr, volume_ratio);
        let confidence = self.compute_confidence(rsi, macd_hist, regime_strength);

        if confidence < self.confidence_minimum as f32 {
            return None;
        }

        if action_score > self.long_entry_threshold as f32 {
            Some(TradeIntent::new(
                features.symbol_id,
                SignalSide::Long,
                SizeHint::Units(1),
                IntentType::Entry,
                confidence as f64,
                action_score as f64,
                self.trade_intent_ttl_ns,
            ))
        } else if action_score < self.short_entry_threshold as f32 {
            Some(TradeIntent::new(
                features.symbol_id,
                SignalSide::Short,
                SizeHint::Units(1),
                IntentType::Entry,
                confidence as f64,
                action_score as f64,
                self.trade_intent_ttl_ns,
            ))
        } else {
            None
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

    fn compute_confidence(&self, rsi: f32, macd_hist: f32, regime_strength: f32) -> f32 {
        let rsi_conf = 1.0 - ((rsi - self.rsi_neutral as f32) / self.rsi_neutral as f32).abs();
        let macd_conf = macd_hist.abs().min(1.0);
        let regime_conf = regime_strength;

        (rsi_conf * self.confidence_rsi_weight as f32
            + macd_conf * self.confidence_macd_weight as f32
            + regime_conf * self.confidence_regime_weight as f32)
            .clamp(0.0, 1.0)
    }
}
