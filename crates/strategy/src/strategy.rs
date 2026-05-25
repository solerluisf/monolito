use model::Prediction;

use crate::{SignalSide, SizeHint, TradeIntent, IntentType, Urgency};

pub struct SignalContext {
    pub symbol: String,
    pub net_position: f64,
    pub current_price: f64,
    pub volatility: f32,
    pub spread_bps: f64,
}

impl SignalContext {
    pub fn new(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
            net_position: 0.0,
            current_price: 0.0,
            volatility: 0.0,
            spread_bps: 0.0,
        }
    }

    pub fn update_price(&mut self, price: f64) {
        self.current_price = price;
    }

    pub fn update_volatility(&mut self, vol: f32) {
        self.volatility = vol;
    }

    pub fn update_spread(&mut self, spread_bps: f64) {
        self.spread_bps = spread_bps;
    }
}

pub trait Strategy: Send + Sync + 'static {
    fn name(&self) -> &str;

    fn evaluate(
        &self,
        prediction: &Prediction,
        ctx: &SignalContext,
    ) -> Option<TradeIntent>;

    fn clone_box(&self) -> Box<dyn Strategy>;
}

impl Clone for Box<dyn Strategy> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

fn compute_action_score(directional_exposure: f32, regime_confidence: f32) -> f32 {
    directional_exposure.abs() * regime_confidence
}

fn urgency_from_score(score: f32) -> Urgency {
    if score > 0.85 {
        Urgency::Aggressive
    } else if score > 0.5 {
        Urgency::Normal
    } else {
        Urgency::Passive
    }
}

pub fn build_entry_intent(
    symbol: &str,
    side: SignalSide,
    confidence: f64,
    action_score: f64,
    units: u64,
) -> TradeIntent {
    let mut intent = TradeIntent::new(
        symbol,
        side,
        SizeHint::Units(units),
        IntentType::Entry,
        confidence,
        action_score,
    );
    intent.urgency = urgency_from_score(action_score as f32);
    intent
}

pub fn build_exit_intent(
    symbol: &str,
    side: SignalSide,
    confidence: f64,
    action_score: f64,
) -> TradeIntent {
    let mut intent = TradeIntent::new(
        symbol,
        side,
        SizeHint::PortfolioPct(1.0),
        IntentType::Exit,
        confidence,
        action_score,
    );
    intent.urgency = urgency_from_score(action_score as f32);
    intent
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_action_score() {
        assert!((compute_action_score(0.9, 0.8) - 0.72).abs() < 0.001);
        assert!((compute_action_score(-0.5, 0.6) - 0.30).abs() < 0.001);
    }

    #[test]
    fn test_urgency_from_score() {
        assert!(matches!(urgency_from_score(0.9), Urgency::Aggressive));
        assert!(matches!(urgency_from_score(0.7), Urgency::Normal));
        assert!(matches!(urgency_from_score(0.2), Urgency::Passive));
    }

    #[test]
    fn test_build_entry_intent() {
        let intent = build_entry_intent("TSLA", SignalSide::Long, 0.75, 0.67, 100);
        assert_eq!(intent.symbol, "TSLA");
        assert!(matches!(intent.side, SignalSide::Long));
        assert!(matches!(intent.size_hint, SizeHint::Units(100)));
        assert!(matches!(intent.intent_type, IntentType::Entry));
    }

    #[test]
    fn test_build_exit_intent() {
        let intent = build_exit_intent("NFLX", SignalSide::Flatten, 0.90, 0.90);
        assert_eq!(intent.symbol, "NFLX");
        assert!(matches!(intent.side, SignalSide::Flatten));
        assert!(matches!(intent.intent_type, IntentType::Exit));
    }
}
