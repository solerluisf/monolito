use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use model::Prediction;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SignalSide {
    Long,
    Short,
    CloseLong,
    CloseShort,
    Flatten,
    Hold,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SizeHint {
    Units(u64),
    Notional(f64),
    PortfolioPct(f64),
    RiskBased(f64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IntentType {
    Entry,
    Exit,
    ScaleIn,
    ScaleOut,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Urgency {
    Passive,
    Normal,
    Aggressive,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeIntent {
    pub intent_id: String,
    pub symbol: String,
    pub side: SignalSide,
    pub size_hint: SizeHint,
    pub intent_type: IntentType,
    pub urgency: Urgency,
    pub confidence: f64,
    pub action_score: f64,
    pub timestamp_ns: u64,
    pub expires_ns: u64,
    pub trace_id: String,
}

impl TradeIntent {
    pub fn new(
        symbol: &str,
        side: SignalSide,
        size_hint: SizeHint,
        intent_type: IntentType,
        confidence: f64,
        action_score: f64,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        Self {
            intent_id: uuid::Uuid::new_v4().to_string(),
            symbol: symbol.to_string(),
            side,
            size_hint,
            intent_type,
            urgency: Urgency::Normal,
            confidence,
            action_score,
            timestamp_ns: now,
            expires_ns: now + 30_000_000_000,
            trace_id: String::new(),
        }
    }

    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        now > self.expires_ns
    }
}

pub struct StrategyEngine {
    pub symbol: String,
    pub long_entry_threshold: f64,
    pub short_entry_threshold: f64,
    pub exit_threshold: f64,
    pub confidence_minimum: f64,
    pub hysteresis_deadband: f64,
    pub entry_cooldown_ms: u64,
    pub exit_cooldown_ms: u64,
    pub prediction_staleness_ns: u64,
    hysteresis_state: AtomicI32,
    last_entry_ns: AtomicU64,
    last_exit_ns: AtomicU64,
    net_position: AtomicU64,
    max_long_units: f64,
    max_short_units: f64,
    pub allow_short: bool,
}

fn f64_to_atomic(val: f64) -> AtomicU64 {
    AtomicU64::new(val.to_bits())
}

fn atomic_to_f64(val: &AtomicU64) -> f64 {
    f64::from_bits(val.load(Ordering::Relaxed))
}

impl StrategyEngine {
    pub fn new(
        symbol: &str,
        long_entry_threshold: f64,
        short_entry_threshold: f64,
        confidence_minimum: f64,
        hysteresis_deadband: f64,
        entry_cooldown_ms: u64,
        exit_cooldown_ms: u64,
        prediction_staleness_ns: u64,
        allow_short: bool,
    ) -> Self {
        Self {
            symbol: symbol.to_string(),
            long_entry_threshold,
            short_entry_threshold,
            exit_threshold: 0.1,
            confidence_minimum,
            hysteresis_deadband,
            entry_cooldown_ms,
            exit_cooldown_ms,
            prediction_staleness_ns,
            hysteresis_state: AtomicI32::new(0),
            last_entry_ns: AtomicU64::new(0),
            last_exit_ns: AtomicU64::new(0),
            net_position: f64_to_atomic(0.0),
            max_long_units: 100.0,
            max_short_units: 100.0,
            allow_short,
        }
    }

    pub fn evaluate(&self, prediction: &Prediction) -> Option<TradeIntent> {
        if prediction.is_stale(self.prediction_staleness_ns) {
            return None;
        }

        if prediction.confidence < self.confidence_minimum as f32 {
            return None;
        }

        let score = prediction.action_score as f64;

        if !self.check_cooldown_for_score(score) {
            return None;
        }

        if let Some(intent) = self.apply_hysteresis(score) {
            if self.check_position_gate(&intent) {
                return Some(intent);
            }
        }

        None
    }

    fn check_cooldown_for_score(&self, score: f64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        if score > self.long_entry_threshold {
            now.saturating_sub(self.last_entry_ns.load(Ordering::Relaxed)) > self.entry_cooldown_ms * 1_000_000
        } else if score < self.short_entry_threshold {
            now.saturating_sub(self.last_entry_ns.load(Ordering::Relaxed)) > self.entry_cooldown_ms * 1_000_000
        } else {
            now.saturating_sub(self.last_exit_ns.load(Ordering::Relaxed)) > self.exit_cooldown_ms * 1_000_000
        }
    }

    fn apply_hysteresis(&self, score: f64) -> Option<TradeIntent> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        match self.hysteresis_state.load(Ordering::Relaxed) {
            0 => {
                if score > self.long_entry_threshold {
                    self.hysteresis_state.store(1, Ordering::Relaxed);
                    self.last_entry_ns.store(now, Ordering::Relaxed);
                    Some(TradeIntent::new(
                        &self.symbol,
                        SignalSide::Long,
                        SizeHint::Units(1),
                        IntentType::Entry,
                        0.5,
                        score,
                    ))
                } else if score < self.short_entry_threshold && self.allow_short {
                    self.hysteresis_state.store(-1, Ordering::Relaxed);
                    self.last_entry_ns.store(now, Ordering::Relaxed);
                    Some(TradeIntent::new(
                        &self.symbol,
                        SignalSide::Short,
                        SizeHint::Units(1),
                        IntentType::Entry,
                        0.5,
                        score,
                    ))
                } else {
                    None
                }
            }
            1 => {
                if score < self.long_entry_threshold - self.hysteresis_deadband {
                    self.hysteresis_state.store(0, Ordering::Relaxed);
                    self.last_exit_ns.store(now, Ordering::Relaxed);
                    Some(TradeIntent::new(
                        &self.symbol,
                        SignalSide::CloseLong,
                        SizeHint::Units(1),
                        IntentType::Exit,
                        0.5,
                        score,
                    ))
                } else {
                    None
                }
            }
            -1 => {
                if score > self.short_entry_threshold + self.hysteresis_deadband {
                    self.hysteresis_state.store(0, Ordering::Relaxed);
                    self.last_exit_ns.store(now, Ordering::Relaxed);
                    Some(TradeIntent::new(
                        &self.symbol,
                        SignalSide::CloseShort,
                        SizeHint::Units(1),
                        IntentType::Exit,
                        0.5,
                        score,
                    ))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn check_cooldown(&self, intent: &TradeIntent) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        match intent.intent_type {
            IntentType::Entry | IntentType::ScaleIn => {
                now.saturating_sub(self.last_entry_ns.load(Ordering::Relaxed)) > self.entry_cooldown_ms * 1_000_000
            }
            IntentType::Exit | IntentType::ScaleOut => {
                now.saturating_sub(self.last_exit_ns.load(Ordering::Relaxed)) > self.exit_cooldown_ms * 1_000_000
            }
        }
    }

    fn check_position_gate(&self, intent: &TradeIntent) -> bool {
        let net_pos = atomic_to_f64(&self.net_position);
        match &intent.side {
            SignalSide::Long => {
                net_pos < self.max_long_units
            }
            SignalSide::Short => {
                self.allow_short && net_pos > -self.max_short_units
            }
            SignalSide::CloseLong | SignalSide::CloseShort | SignalSide::Flatten | SignalSide::Hold => true,
        }
    }

    pub fn update_position(&self, delta: f64) {
        let current = atomic_to_f64(&self.net_position);
        self.net_position.store((current + delta).to_bits(), Ordering::Relaxed);
    }

    pub fn net_position(&self) -> f64 {
        atomic_to_f64(&self.net_position)
    }

    pub fn reset_hysteresis(&self) {
        self.hysteresis_state.store(0, Ordering::Relaxed);
    }
}

use crate::strategy::{Strategy, SignalContext};

impl Strategy for StrategyEngine {
    fn name(&self) -> &str {
        "threshold_hysteresis"
    }

    fn evaluate(
        &self,
        prediction: &Prediction,
        _ctx: &SignalContext,
    ) -> Option<TradeIntent> {
        StrategyEngine::evaluate(self, prediction)
    }

    fn clone_box(&self) -> Box<dyn Strategy> {
        Box::new(Self {
            symbol: self.symbol.clone(),
            long_entry_threshold: self.long_entry_threshold,
            short_entry_threshold: self.short_entry_threshold,
            exit_threshold: self.exit_threshold,
            confidence_minimum: self.confidence_minimum,
            hysteresis_deadband: self.hysteresis_deadband,
            entry_cooldown_ms: self.entry_cooldown_ms,
            exit_cooldown_ms: self.exit_cooldown_ms,
            prediction_staleness_ns: self.prediction_staleness_ns,
            hysteresis_state: AtomicI32::new(self.hysteresis_state.load(Ordering::Relaxed)),
            last_entry_ns: AtomicU64::new(self.last_entry_ns.load(Ordering::Relaxed)),
            last_exit_ns: AtomicU64::new(self.last_exit_ns.load(Ordering::Relaxed)),
            net_position: AtomicU64::new(self.net_position.load(Ordering::Relaxed)),
            max_long_units: self.max_long_units,
            max_short_units: self.max_short_units,
            allow_short: self.allow_short,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::Prediction;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn make_pred(score: f64, confidence: f64) -> Prediction {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        Prediction {
            symbol: "AAPL".to_string(),
            forecast: score as f32,
            confidence: confidence as f32,
            action_score: score as f32,
            regime_label: 0,
            regime_strength: 0.5,
            computed_ns: now,
        }
    }

    #[test]
    fn test_strategy_long_entry() {
        let engine = StrategyEngine::new(
            "AAPL", 0.6, -0.6, 0.5, 0.15, 5000, 2000, 150_000_000, true,
        );
        let pred = make_pred(0.8, 0.9);
        let intent = engine.evaluate(&pred);
        assert!(intent.is_some());
        let intent = intent.unwrap();
        assert!(matches!(intent.side, SignalSide::Long));
    }

    #[test]
    fn test_strategy_no_signal_below_threshold() {
        let engine = StrategyEngine::new(
            "AAPL", 0.6, -0.6, 0.5, 0.15, 5000, 2000, 150_000_000, true,
        );
        let pred = make_pred(0.3, 0.9);
        let intent = engine.evaluate(&pred);
        assert!(intent.is_none());
    }

    #[test]
    fn test_strategy_low_confidence_rejected() {
        let engine = StrategyEngine::new(
            "AAPL", 0.6, -0.6, 0.5, 0.15, 5000, 2000, 150_000_000, true,
        );
        let pred = make_pred(0.8, 0.3);
        let intent = engine.evaluate(&pred);
        assert!(intent.is_none());
    }

    #[test]
    fn test_strategy_hysteresis_prevents_flip() {
        let engine = StrategyEngine::new(
            "AAPL", 0.6, -0.6, 0.5, 0.15, 5000, 2000, 150_000_000, true,
        );
        let pred_long = make_pred(0.8, 0.9);
        engine.evaluate(&pred_long);
        assert_eq!(engine.hysteresis_state.load(Ordering::Relaxed), 1);

        let pred_weak = make_pred(0.5, 0.9);
        let intent = engine.evaluate(&pred_weak);
        assert!(intent.is_none());
    }

    #[test]
    fn test_strategy_cooldown() {
        let engine = StrategyEngine::new(
            "AAPL", 0.6, -0.6, 0.5, 0.15, 5000, 2000, 150_000_000, true,
        );
        let pred = make_pred(0.8, 0.9);
        engine.evaluate(&pred);

        let pred2 = make_pred(0.85, 0.9);
        let intent = engine.evaluate(&pred2);
        assert!(intent.is_none());
    }

    #[test]
    fn test_strategy_position_gate() {
        let engine = StrategyEngine::new(
            "AAPL", 0.6, -0.6, 0.5, 0.15, 0, 0, 150_000_000, true,
        );
        engine.update_position(101.0);
        let pred = make_pred(0.8, 0.9);
        let intent = engine.evaluate(&pred);
        assert!(intent.is_none());
    }
}
