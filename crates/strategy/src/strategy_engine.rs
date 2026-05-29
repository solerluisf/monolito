use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;

use model::Prediction;
use unified_trading_core::clock::{Clock, WallClock};
use unified_trading_core::symbol_registry::SymbolId;
use unified_trading_core::symbol_registry::next_intent_id;

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
    pub intent_id: u64,
    pub symbol_id: SymbolId,
    pub side: SignalSide,
    pub size_hint: SizeHint,
    pub intent_type: IntentType,
    pub urgency: Urgency,
    pub confidence: f64,
    pub action_score: f64,
    pub timestamp_ns: u64,
    pub expires_ns: u64,
    pub trace_id: u64,
}

impl TradeIntent {
    pub fn new(
        symbol_id: SymbolId,
        side: SignalSide,
        size_hint: SizeHint,
        intent_type: IntentType,
        confidence: f64,
        action_score: f64,
        ttl_ns: u64,
        trace_id: u64,
    ) -> Self {
        Self::with_clock(
            symbol_id,
            side,
            size_hint,
            intent_type,
            confidence,
            action_score,
            ttl_ns,
            trace_id,
            Arc::new(wall_clock()),
        )
    }

    pub fn with_clock(
        symbol_id: SymbolId,
        side: SignalSide,
        size_hint: SizeHint,
        intent_type: IntentType,
        confidence: f64,
        action_score: f64,
        ttl_ns: u64,
        trace_id: u64,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let now = clock.now_ns();

        Self {
            intent_id: next_intent_id(),
            symbol_id,
            side,
            size_hint,
            intent_type,
            urgency: Urgency::Normal,
            confidence,
            action_score,
            timestamp_ns: now,
            expires_ns: now + ttl_ns,
            trace_id,
        }
    }

    pub fn is_expired(&self) -> bool {
        Self::is_expired_with_clock(self, Arc::new(wall_clock()))
    }

    pub fn is_expired_with_clock(&self, clock: Arc<dyn Clock>) -> bool {
        let now = clock.now_ns();
        now > self.expires_ns
    }
}

/// Returns a new WallClock instance for use in production code.
fn wall_clock() -> WallClock {
    WallClock::new()
}

pub struct StrategyEngine {
    pub symbol_id: SymbolId,
    pub long_entry_threshold: f64,
    pub short_entry_threshold: f64,
    pub exit_threshold: f64,
    pub confidence_minimum: f64,
    pub hysteresis_deadband: f64,
    pub entry_cooldown_ms: u64,
    pub exit_cooldown_ms: u64,
    pub prediction_staleness_ns: u64,
    pub trade_intent_ttl_ns: u64,
    hysteresis_state: AtomicI32,
    last_entry_ns: AtomicU64,
    last_exit_ns: AtomicU64,
    net_position: AtomicU64,
    pub max_long_units: f64,
    pub max_short_units: f64,
    pub allow_short: bool,
    pub urgency_aggressive_threshold: f64,
    pub urgency_normal_threshold: f64,
    pub action_score_rsi_weight: f64,
    pub action_score_macd_weight: f64,
    pub action_score_volatility_weight: f64,
    pub atr_penalty_threshold: f64,
    pub atr_penalty_value: f64,
    pub rsi_overbought: f64,
    pub rsi_oversold: f64,
    pub rsi_neutral: f64,
    pub confidence_rsi_weight: f64,
    pub confidence_macd_weight: f64,
    pub confidence_regime_weight: f64,
    pub volume_ratio_clamp: f64,
}

fn f64_to_atomic(val: f64) -> AtomicU64 {
    AtomicU64::new(val.to_bits())
}

fn atomic_to_f64(val: &AtomicU64) -> f64 {
    f64::from_bits(val.load(Ordering::Relaxed))
}

impl StrategyEngine {
    pub fn new(symbol_id: SymbolId, config: &unified_trading_core::config::StrategyConfig) -> Self {
        Self {
            symbol_id,
            long_entry_threshold: config.long_entry_threshold,
            short_entry_threshold: config.short_entry_threshold,
            exit_threshold: config.exit_threshold,
            confidence_minimum: config.confidence_minimum,
            hysteresis_deadband: config.hysteresis_deadband,
            entry_cooldown_ms: config.entry_cooldown_ms,
            exit_cooldown_ms: config.exit_cooldown_ms,
            prediction_staleness_ns: config.prediction_staleness_ns,
            trade_intent_ttl_ns: config.trade_intent_ttl_ns,
            hysteresis_state: AtomicI32::new(0),
            last_entry_ns: AtomicU64::new(0),
            last_exit_ns: AtomicU64::new(0),
            net_position: f64_to_atomic(0.0),
            max_long_units: config.max_long_units,
            max_short_units: config.max_short_units,
            allow_short: config.allow_short,
            urgency_aggressive_threshold: config.urgency_aggressive_threshold,
            urgency_normal_threshold: config.urgency_normal_threshold,
            action_score_rsi_weight: config.action_score_rsi_weight,
            action_score_macd_weight: config.action_score_macd_weight,
            action_score_volatility_weight: config.action_score_volatility_weight,
            atr_penalty_threshold: config.atr_penalty_threshold,
            atr_penalty_value: config.atr_penalty_value,
            rsi_overbought: config.rsi_overbought,
            rsi_oversold: config.rsi_oversold,
            rsi_neutral: config.rsi_neutral,
            confidence_rsi_weight: config.confidence_rsi_weight,
            confidence_macd_weight: config.confidence_macd_weight,
            confidence_regime_weight: config.confidence_regime_weight,
            volume_ratio_clamp: config.volume_ratio_clamp,
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

        if let Some(intent) = self.apply_hysteresis(score, prediction.trace_id) {
            if self.check_position_gate(&intent) {
                return Some(intent);
            }
        }

        None
    }

    fn check_cooldown_for_score(&self, score: f64) -> bool {
        let now = unified_trading_core::clock::wall_time_ns();

        if score > self.long_entry_threshold {
            now.saturating_sub(self.last_entry_ns.load(Ordering::Relaxed)) > self.entry_cooldown_ms * 1_000_000
        } else if score < self.short_entry_threshold {
            now.saturating_sub(self.last_entry_ns.load(Ordering::Relaxed)) > self.entry_cooldown_ms * 1_000_000
        } else {
            now.saturating_sub(self.last_exit_ns.load(Ordering::Relaxed)) > self.exit_cooldown_ms * 1_000_000
        }
    }

    fn apply_hysteresis(&self, score: f64, trace_id: u64) -> Option<TradeIntent> {
        let now = unified_trading_core::clock::wall_time_ns();

        match self.hysteresis_state.load(Ordering::Relaxed) {
            0 => {
                if score > self.long_entry_threshold {
                    self.hysteresis_state.store(1, Ordering::Relaxed);
                    self.last_entry_ns.store(now, Ordering::Relaxed);
                    Some(TradeIntent::new(
                        self.symbol_id,
                        SignalSide::Long,
                        SizeHint::Units(1),
                        IntentType::Entry,
                        0.5,
                        score,
                        self.trade_intent_ttl_ns,
                        trace_id,
                    ))
                } else if score < self.short_entry_threshold && self.allow_short {
                    self.hysteresis_state.store(-1, Ordering::Relaxed);
                    self.last_entry_ns.store(now, Ordering::Relaxed);
                    Some(TradeIntent::new(
                        self.symbol_id,
                        SignalSide::Short,
                        SizeHint::Units(1),
                        IntentType::Entry,
                        0.5,
                        score,
                        self.trade_intent_ttl_ns,
                        trace_id,
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
                        self.symbol_id,
                        SignalSide::CloseLong,
                        SizeHint::Units(1),
                        IntentType::Exit,
                        0.5,
                        score,
                        self.trade_intent_ttl_ns,
                        trace_id,
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
                        self.symbol_id,
                        SignalSide::CloseShort,
                        SizeHint::Units(1),
                        IntentType::Exit,
                        0.5,
                        score,
                        self.trade_intent_ttl_ns,
                        trace_id,
                    ))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn check_cooldown(&self, intent: &TradeIntent) -> bool {
        let now = unified_trading_core::clock::wall_time_ns();

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
        let config = unified_trading_core::config::StrategyConfig {
            long_entry_threshold: self.long_entry_threshold,
            short_entry_threshold: self.short_entry_threshold,
            exit_threshold: self.exit_threshold,
            confidence_minimum: self.confidence_minimum,
            hysteresis_deadband: self.hysteresis_deadband,
            entry_cooldown_ms: self.entry_cooldown_ms,
            exit_cooldown_ms: self.exit_cooldown_ms,
            prediction_staleness_ns: self.prediction_staleness_ns,
            trade_intent_ttl_ns: self.trade_intent_ttl_ns,
            max_long_units: self.max_long_units,
            max_short_units: self.max_short_units,
            urgency_aggressive_threshold: self.urgency_aggressive_threshold,
            urgency_normal_threshold: self.urgency_normal_threshold,
            allow_short: self.allow_short,
            action_score_rsi_weight: self.action_score_rsi_weight,
            action_score_macd_weight: self.action_score_macd_weight,
            action_score_volatility_weight: self.action_score_volatility_weight,
            atr_penalty_threshold: self.atr_penalty_threshold,
            atr_penalty_value: self.atr_penalty_value,
            rsi_overbought: self.rsi_overbought,
            rsi_oversold: self.rsi_oversold,
            rsi_neutral: self.rsi_neutral,
            confidence_rsi_weight: self.confidence_rsi_weight,
            confidence_macd_weight: self.confidence_macd_weight,
            confidence_regime_weight: self.confidence_regime_weight,
            volume_ratio_clamp: self.volume_ratio_clamp,
        };
        let mut engine = Self::new(self.symbol_id, &config);
        engine.hysteresis_state = AtomicI32::new(self.hysteresis_state.load(Ordering::Relaxed));
        engine.last_entry_ns = AtomicU64::new(self.last_entry_ns.load(Ordering::Relaxed));
        engine.last_exit_ns = AtomicU64::new(self.last_exit_ns.load(Ordering::Relaxed));
        engine.net_position = AtomicU64::new(self.net_position.load(Ordering::Relaxed));
        Box::new(engine)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::Prediction;
    use std::sync::Arc;
    use unified_trading_core::clock::TestClock;

    fn make_engine() -> StrategyEngine {
        let config = unified_trading_core::config::StrategyConfig {
            long_entry_threshold: 0.6,
            short_entry_threshold: -0.6,
            exit_threshold: 0.1,
            confidence_minimum: 0.5,
            hysteresis_deadband: 0.15,
            entry_cooldown_ms: 5000,
            exit_cooldown_ms: 2000,
            prediction_staleness_ns: 150_000_000,
            trade_intent_ttl_ns: 30_000_000_000,
            max_long_units: 100.0,
            max_short_units: 100.0,
            urgency_aggressive_threshold: 0.85,
            urgency_normal_threshold: 0.5,
            allow_short: true,
            action_score_rsi_weight: 0.4,
            action_score_macd_weight: 0.4,
            action_score_volatility_weight: 0.2,
            atr_penalty_threshold: 2.0,
            atr_penalty_value: -0.2,
            rsi_overbought: 70.0,
            rsi_oversold: 30.0,
            rsi_neutral: 50.0,
            confidence_rsi_weight: 0.3,
            confidence_macd_weight: 0.4,
            confidence_regime_weight: 0.3,
            volume_ratio_clamp: 0.3,
        };
        StrategyEngine::new(SymbolId::from_raw(0), &config)
    }

    fn make_pred(score: f64, confidence: f64) -> Prediction {
        let now = unified_trading_core::clock::wall_time_ns();
        Prediction {
            symbol_id: SymbolId::from_raw(0),
            forecast: score as f32,
            confidence: confidence as f32,
            action_score: score as f32,
            regime_label: 0,
            regime_strength: 0.5,
            computed_ns: now,
            trace_id: 0,
        }
    }

    fn make_pred_with_trace(score: f64, confidence: f64, trace_id: u64) -> Prediction {
        let now = unified_trading_core::clock::wall_time_ns();
        Prediction {
            symbol_id: SymbolId::from_raw(0),
            forecast: score as f32,
            confidence: confidence as f32,
            action_score: score as f32,
            regime_label: 0,
            regime_strength: 0.5,
            computed_ns: now,
            trace_id,
        }
    }

    #[test]
    fn test_trade_intent_expiration_with_test_clock() {
        // This is the acceptance test for ISSUE-032:
        // A unit test advances TestClock by 1 hour and asserts that
        // TradeIntent::is_expired() triggers correctly.
        
        let clock = Arc::new(TestClock::new(1_000_000_000_000)); // 1 second after epoch (1e12 ns)
        
        // Create a TradeIntent with 30 second TTL
        let ttl_ns = 30_000_000_000u64; // 30 seconds in nanoseconds
        let intent = TradeIntent::with_clock(
            SymbolId::from_raw(0),
            SignalSide::Long,
            SizeHint::Units(1),
            IntentType::Entry,
            0.8,
            0.8,
            ttl_ns,
            0,
            Arc::clone(&clock) as Arc<dyn Clock>,
        );
        
        // At creation time, intent should not be expired
        assert!(!intent.is_expired_with_clock(Arc::clone(&clock) as Arc<dyn Clock>), 
            "Intent should not be expired at creation time");
        
        // Advance clock by 1 second (well within TTL)
        clock.advance(1_000_000_000);
        assert!(!intent.is_expired_with_clock(Arc::clone(&clock) as Arc<dyn Clock>),
            "Intent should not be expired after 1 second");
        
        // Advance clock by 1 hour (well past TTL)
        let one_hour_ns = 3_600_000_000_000u64; // 1 hour in nanoseconds
        clock.advance(one_hour_ns);
        assert!(intent.is_expired_with_clock(Arc::clone(&clock) as Arc<dyn Clock>),
            "Intent should be expired after advancing clock by 1 hour");
    }

    #[test]
    fn test_trade_intent_expiration_edge_case() {
        // Test the exact boundary of expiration
        let clock = Arc::new(TestClock::new(1_000_000_000_000));
        
        // Create intent with exactly 1 second TTL
        let ttl_ns = 1_000_000_000u64; // 1 second
        let intent = TradeIntent::with_clock(
            SymbolId::from_raw(0),
            SignalSide::Long,
            SizeHint::Units(1),
            IntentType::Entry,
            0.8,
            0.8,
            ttl_ns,
            0,
            Arc::clone(&clock) as Arc<dyn Clock>,
        );
        
        // At exactly TTL time, should not be expired (expires_ns > now)
        assert!(!intent.is_expired_with_clock(Arc::clone(&clock) as Arc<dyn Clock>));
        
        // Advance by 1 nanosecond past TTL (TTL is 1e9 ns, so need TTL + 1)
        clock.advance(ttl_ns + 1);
        assert!(intent.is_expired_with_clock(Arc::clone(&clock) as Arc<dyn Clock>),
            "Intent should be expired 1ns past TTL");
    }

    #[test]
    fn test_strategy_long_entry() {
        let engine = make_engine();
        let pred = make_pred(0.8, 0.9);
        let intent = engine.evaluate(&pred);
        assert!(intent.is_some());
        let intent = intent.unwrap();
        assert!(matches!(intent.side, SignalSide::Long));
    }

    #[test]
    fn test_strategy_no_signal_below_threshold() {
        let engine = make_engine();
        let pred = make_pred(0.3, 0.9);
        let intent = engine.evaluate(&pred);
        assert!(intent.is_none());
    }

    #[test]
    fn test_strategy_low_confidence_rejected() {
        let engine = make_engine();
        let pred = make_pred(0.8, 0.3);
        let intent = engine.evaluate(&pred);
        assert!(intent.is_none());
    }

    #[test]
    fn test_strategy_hysteresis_prevents_flip() {
        let engine = make_engine();
        let pred_long = make_pred(0.8, 0.9);
        engine.evaluate(&pred_long);
        assert_eq!(engine.hysteresis_state.load(Ordering::Relaxed), 1);

        let pred_weak = make_pred(0.5, 0.9);
        let intent = engine.evaluate(&pred_weak);
        assert!(intent.is_none());
    }

    #[test]
    fn test_strategy_cooldown() {
        let engine = make_engine();
        let pred = make_pred(0.8, 0.9);
        engine.evaluate(&pred);

        let pred2 = make_pred(0.85, 0.9);
        let intent = engine.evaluate(&pred2);
        assert!(intent.is_none());
    }

    #[test]
    fn test_strategy_position_gate() {
        let engine = make_engine();
        engine.update_position(101.0);
        let pred = make_pred(0.8, 0.9);
        let intent = engine.evaluate(&pred);
        assert!(intent.is_none());
    }

    #[test]
    fn test_strategy_trace_id_propagation() {
        let engine = make_engine();
        let trace_id = 12345u64;
        let pred = make_pred_with_trace(0.8, 0.9, trace_id);
        let intent = engine.evaluate(&pred);
        assert!(intent.is_some());
        let intent = intent.unwrap();
        assert_eq!(intent.trace_id, trace_id);
    }

    #[test]
    fn test_strategy_trace_id_propagation_short() {
        let engine = make_engine();
        let trace_id = 67890u64;
        let pred = make_pred_with_trace(-0.8, 0.9, trace_id);
        let intent = engine.evaluate(&pred);
        assert!(intent.is_some());
        let intent = intent.unwrap();
        assert_eq!(intent.trace_id, trace_id);
    }

    #[test]
    fn test_strategy_trace_id_propagation_exit() {
        let engine = make_engine();
        // First enter a long position
        let pred_enter = make_pred_with_trace(0.8, 0.9, 111);
        engine.evaluate(&pred_enter);
        
        // Then trigger an exit
        let pred_exit = make_pred_with_trace(0.3, 0.9, 222);
        let intent = engine.evaluate(&pred_exit);
        assert!(intent.is_some());
        let intent = intent.unwrap();
        assert_eq!(intent.trace_id, 222);
    }
}
