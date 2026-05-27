pub mod kill_switch;
pub mod config;
pub mod metrics;
pub mod journal;
pub mod heartbeat;
pub mod command_channel;
pub mod config_watcher;
pub mod ws;
pub mod threading;
pub mod validator;
pub mod idempotency;
pub mod position_manager;
pub mod portfolio_manager;
pub mod symbol_registry;
pub mod channel_utils;
pub mod large_pages;
pub mod crash_detector;

pub use kill_switch::KillSwitch;
pub use metrics::{GlobalMetrics, MetricsSnapshot};
pub use config::{EngineConfig, CheckSeverity, default_check_severities};
pub use journal::JournalWriter;
pub use journal::JournalEntry;
pub use journal::JournalCommand;
pub use heartbeat::ThreadHeartbeatMonitor;
pub use command_channel::{CommandChannel, CommandActor, ControlCommand, ControlResponse, StrategyParams,
    RiskConfigUpdate, BrokerConfigUpdate, FeatureConfigUpdate, ModelConfigUpdate, JournalConfigUpdate,
    AssetConfigUpdate, ExecutionDefaultsUpdate, CircuitBreakerConfigUpdate, RateLimitConfigUpdate,
    ChannelConfigUpdate, ReactorConfigUpdate, ValidatorConfigUpdate
};
pub use config_watcher::ConfigWatcher;
pub use ws::{create_ws_router, WsState};
pub use threading::{pin_to_core, set_thread_priority, spawn_pinned, ThreadPriority, PinError};
pub use validator::RequestValidator;
pub use idempotency::IdempotencyStore;
pub use position_manager::PositionManager;
pub use portfolio_manager::PortfolioManager;
pub use symbol_registry::{SymbolRegistry, SymbolId, SymbolIdArray, MAX_SYMBOLS};
pub use large_pages::{enable_large_pages, allocate_large_pages, log_large_page_result, LargePageResult};
pub use crash_detector::CrashDetector;
