pub mod strategy_engine;
pub mod strategy;
pub mod hysteresis;
pub mod cooldown;
pub mod strategy_ext;

pub use strategy_engine::{
    TradeIntent, SignalSide, SizeHint, IntentType, Urgency, StrategyEngine,
};
pub use strategy::{Strategy, SignalContext, build_entry_intent, build_exit_intent};
pub use hysteresis::HysteresisFilter;
pub use cooldown::CooldownTracker;
pub use strategy_ext::StrategyEngineExt;
