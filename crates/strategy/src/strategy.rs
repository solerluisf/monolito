use model::Prediction;
use unified_trading_core::symbol_registry::SymbolId;

use crate::{SignalSide, SizeHint, TradeIntent, IntentType, Urgency};

pub struct SignalContext {
    pub symbol_id: SymbolId,
    pub net_position: f64,
    pub current_price: f64,
    pub volatility: f32,
    pub spread_bps: f64,
}

impl SignalContext {
    pub fn new(symbol_id: SymbolId) -> Self {
        Self {
            symbol_id,
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

pub fn urgency_from_score(score: f32, aggressive_threshold: f32, normal_threshold: f32) -> Urgency {
    if score > aggressive_threshold {
        Urgency::Aggressive
    } else if score > normal_threshold {
        Urgency::Normal
    } else {
        Urgency::Passive
    }
}

pub fn build_entry_intent(
    symbol_id: SymbolId,
    side: SignalSide,
    confidence: f64,
    action_score: f64,
    units: u64,
    ttl_ns: u64,
    aggressive_threshold: f32,
    normal_threshold: f32,
) -> TradeIntent {
    let mut intent = TradeIntent::new(
        symbol_id,
        side,
        SizeHint::Units(units),
        IntentType::Entry,
        confidence,
        action_score,
        ttl_ns,
    );
    intent.urgency = urgency_from_score(action_score as f32, aggressive_threshold, normal_threshold);
    intent
}

pub fn build_exit_intent(
    symbol_id: SymbolId,
    side: SignalSide,
    confidence: f64,
    action_score: f64,
    ttl_ns: u64,
    aggressive_threshold: f32,
    normal_threshold: f32,
) -> TradeIntent {
    let mut intent = TradeIntent::new(
        symbol_id,
        side,
        SizeHint::PortfolioPct(1.0),
        IntentType::Exit,
        confidence,
        action_score,
        ttl_ns,
    );
    intent.urgency = urgency_from_score(action_score as f32, aggressive_threshold, normal_threshold);
    intent
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_urgency_from_score() {
        assert!(matches!(urgency_from_score(0.9, 0.85, 0.5), Urgency::Aggressive));
        assert!(matches!(urgency_from_score(0.7, 0.85, 0.5), Urgency::Normal));
        assert!(matches!(urgency_from_score(0.2, 0.85, 0.5), Urgency::Passive));
    }

    #[test]
    fn test_build_entry_intent() {
        let sid = SymbolId::from_raw(0);
        let intent = build_entry_intent(sid, SignalSide::Long, 0.75, 0.67, 100, 30_000_000_000, 0.85, 0.5);
        assert_eq!(intent.symbol_id.as_u16(), 0);
        assert!(matches!(intent.side, SignalSide::Long));
        assert!(matches!(intent.size_hint, SizeHint::Units(100)));
        assert!(matches!(intent.intent_type, IntentType::Entry));
    }

    #[test]
    fn test_build_exit_intent() {
        let sid = SymbolId::from_raw(0);
        let intent = build_exit_intent(sid, SignalSide::Flatten, 0.90, 0.90, 30_000_000_000, 0.85, 0.5);
        assert_eq!(intent.symbol_id.as_u16(), 0);
        assert!(matches!(intent.side, SignalSide::Flatten));
        assert!(matches!(intent.intent_type, IntentType::Exit));
    }
}
