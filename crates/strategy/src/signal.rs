use std::sync::Arc;

use feature::NormalizedTick;

use crate::{SignalSide, SizeHint, TradeIntent};

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

    pub fn update(&mut self, tick: &NormalizedTick) {
        self.current_price = tick.mid;
        if tick.atr > 0.0 {
            self.volatility = tick.atr / tick.mid as f32;
        }
        if tick.mid > 0.0 {
            let spread = (tick.ask - tick.bid) / tick.mid * 10000.0;
            self.spread_bps = spread.max(0.0);
        }
    }
}

fn compute_action_score(directional_exposure: f32, regime_confidence: f32) -> f32 {
    directional_exposure.abs() * regime_confidence
}

fn urgency_from_score(score: f32) -> crate::Urgency {
    if score > 0.85 {
        crate::Urgency::Aggressive
    } else if score > 0.5 {
        crate::Urgency::Normal
    } else {
        crate::Urgency::Passive
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SizeHint, IntentType, SignalSide};

    #[test]
    fn test_compute_action_score() {
        assert!((compute_action_score(0.9, 0.8) - 0.72).abs() < 0.001);
        assert!((compute_action_score(-0.5, 0.6) - 0.30).abs() < 0.001);
    }

    #[test]
    fn test_urgency_from_score() {
        assert!(matches!(urgency_from_score(0.9), crate::Urgency::Aggressive));
        assert!(matches!(urgency_from_score(0.7), crate::Urgency::Normal));
        assert!(matches!(urgency_from_score(0.2), crate::Urgency::Passive));
    }

    #[test]
    fn test_build_entry_intent_long() {
        intent_build_tests::build_entry_intent_long();
    }

    #[test]
    fn test_build_entry_intent_short() {
        intent_build_tests::build_entry_intent_short();
    }

    #[test]
    fn test_build_exit_intent() {
        intent_build_tests::build_exit_intent();
    }

    mod intent_build_tests {
        use super::*;

        pub fn build_entry_intent_long() {
            use crate::{SizeHint, IntentType, SignalSide};
            let intent = build_entry_intent("TSLA", SignalSide::Long, 0.75, 100.0);
            assert_eq!(intent.symbol, "TSLA");
            assert!(matches!(intent.side, SignalSide::Long));
            assert!(matches!(intent.size_hint, SizeHint::Units(100)));
            assert!(matches!(intent.intent_type, IntentType::Entry));
            assert!((intent.confidence - 0.75).abs() < 0.01);
        }

        pub fn build_entry_intent_short() {
            let intent = build_entry_intent("AMZN", SignalSide::Short, 0.65, 50.0);
            assert_eq!(intent.symbol, "AMZN");
            assert!(matches!(intent.side, SignalSide::Short));
            assert!(matches!(intent.size_hint, SizeHint::Units(50)));
        }

        pub fn build_exit_intent() {
            let intent = build_exit_intent("NFLX", SignalSide::Flatten, 0.90);
            assert_eq!(intent.symbol, "NFLX");
            assert!(matches!(intent.side, SignalSide::Flatten));
            assert!(matches!(intent.intent_type, IntentType::Exit));
            assert!((intent.confidence - 0.90).abs() < 0.01);
        }
    }

    fn build_entry_intent(symbol: &str, side: SignalSide, confidence: f64, units: u64) -> TradeIntent {
        TradeIntent::new(
            symbol,
            side,
            SizeHint::Units(units),
            IntentType::Entry,
            confidence,
            confidence * 0.9,
        )
    }

    fn build_exit_intent(symbol: &str, side: SignalSide, confidence: f64) -> TradeIntent {
        TradeIntent::new(
            symbol,
            side,
            SizeHint::PortfolioPct(1.0),
            IntentType::Exit,
            confidence,
            confidence,
        )
    }
}
