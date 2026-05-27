use crossbeam_channel::{bounded, Receiver, Sender};
use std::thread;

use crate::threading::{spawn_pinned, ThreadPriority};

#[derive(Debug, Clone)]
pub struct StrategyParams {
    pub long_entry_threshold: f64,
    pub short_entry_threshold: f64,
    pub exit_threshold: f64,
    pub confidence_minimum: f64,
    pub hysteresis_deadband: f64,
    pub entry_cooldown_ms: u64,
    pub exit_cooldown_ms: u64,
    pub prediction_staleness_ns: u64,
    pub allow_short: bool,
    pub max_long_units: f64,
    pub max_short_units: f64,
    pub trade_intent_ttl_ns: u64,
    pub urgency_aggressive_threshold: f32,
    pub urgency_normal_threshold: f32,
}

#[derive(Debug, Clone)]
pub struct RiskConfigUpdate {
    pub max_portfolio_exposure: f64,
    pub max_leverage: f64,
    pub max_drawdown_pct: f64,
    pub max_order_rate_per_sec: u32,
    pub max_position_per_symbol: f64,
    pub max_volatility: f64,
    pub max_spread_bps: f64,
    pub max_slippage_bps: f64,
    pub allow_short: bool,
    pub kill_switch_on_drawdown: bool,
    pub risk_intent_staleness_ns: u64,
    pub initial_equity: f64,
}

#[derive(Debug, Clone)]
pub struct BrokerConfigUpdate {
    pub broker_type: String,
    pub paper_trading: bool,
    pub ws_url: String,
    pub rest_url: String,
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
}

#[derive(Debug, Clone)]
pub struct FeatureConfigUpdate {
    pub rsi_period: usize,
    pub macd_fast: usize,
    pub macd_slow: usize,
    pub macd_signal: usize,
    pub atr_period: usize,
    pub ema_periods: Vec<usize>,
    pub rolling_window_sizes: Vec<usize>,
    pub price_window_size: usize,
    pub volume_window_size: usize,
    pub regime_volatile_atr_threshold: f64,
    pub regime_trending_threshold: f32,
}

#[derive(Debug, Clone)]
pub struct ModelConfigUpdate {
    pub model_dir: String,
    pub inference_threads: usize,
    pub max_inference_latency_ms: u64,
    pub feature_vector_size: usize,
    pub inference_rsi_bearish_threshold: f32,
    pub inference_rsi_bullish_threshold: f32,
    pub inference_rsi_center: f32,
    pub inference_atr_penalty_threshold: f32,
    pub inference_volume_confirmation_threshold: f32,
    pub action_score_rsi_weight: f32,
    pub action_score_macd_weight: f32,
    pub action_score_volatility_weight: f32,
    pub confidence_rsi_weight: f32,
    pub confidence_macd_weight: f32,
    pub confidence_regime_weight: f32,
}

#[derive(Debug, Clone)]
pub struct JournalConfigUpdate {
    pub journal_dir: String,
    pub flush_interval_ms: u64,
    pub snapshot_interval_sec: u64,
    pub max_file_size_mb: u64,
}

#[derive(Debug, Clone)]
pub struct AssetConfigUpdate {
    pub enabled: bool,
    pub max_position: f64,
    pub tick_size: f64,
}

#[derive(Debug, Clone)]
pub struct ExecutionDefaultsUpdate {
    pub default_order_quantity: f64,
    pub execution_per_symbol_rate_divisor: f64,
}

#[derive(Debug, Clone)]
pub struct CircuitBreakerConfigUpdate {
    pub failure_threshold: u64,
    pub cooldown_ms: u64,
}

#[derive(Debug, Clone)]
pub struct RateLimitConfigUpdate {
    pub global_rate: f64,
    pub per_symbol_rate: f64,
}

#[derive(Debug, Clone)]
pub struct ChannelConfigUpdate {
    pub per_asset_tick_channel_capacity: usize,
    pub feature_channel_capacity: usize,
    pub risk_channel_capacity: usize,
    pub decision_channel_capacity: usize,
    pub lifecycle_channel_capacity: usize,
    pub command_channel_capacity: usize,
    pub journal_channel_capacity: usize,
}

#[derive(Debug, Clone)]
pub struct ReactorConfigUpdate {
    pub max_batch_size: usize,
    pub control_batch_size: usize,
    pub sleep_on_empty_us: u64,
    pub backpressure_log_interval: u64,
}

#[derive(Debug, Clone)]
pub struct ValidatorConfigUpdate {
    pub max_symbol_length: usize,
    pub max_quantity: f64,
    pub max_order_id_length: usize,
}

#[derive(Debug, Clone)]
pub enum ControlCommand {
    SetKillSwitch(bool),
    UpdateConfig(String),
    PauseAsset(String),
    ResumeAsset(String),
    SetMode(String),
    SwapStrategy {
        symbol: String,
        strategy_type: String,
        params: Option<StrategyParams>,
    },
    CircuitBreakerTrip,
    CircuitBreakerReset,
    SetRiskParams(RiskConfigUpdate),
    SetBrokerParams(BrokerConfigUpdate),
    SetFeatureParams(FeatureConfigUpdate),
    SetModelParams(ModelConfigUpdate),
    SetJournalParams(JournalConfigUpdate),
    SetAssetConfig { symbol: String, config: AssetConfigUpdate },
    SetExecutionDefaults(ExecutionDefaultsUpdate),
    SetCircuitBreakerParams(CircuitBreakerConfigUpdate),
    SetRateLimits(RateLimitConfigUpdate),
    SetChannelParams(ChannelConfigUpdate),
    SetReactorParams(ReactorConfigUpdate),
    SetValidatorParams(ValidatorConfigUpdate),
    ModelSwap(String),
    ReloadConfig,
    FlushJournal,
    SubscribeFeed { symbol: String },
    UnsubscribeFeed { symbol: String },
    GetStatus,
    Shutdown,
}

#[derive(Debug, Clone)]
pub enum ControlResponse {
    Ok,
    Error(String),
    Status(String),
}

pub struct CommandChannel {
    pub tx: Sender<ControlCommand>,
    pub rx: Receiver<ControlCommand>,
}

impl CommandChannel {
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = bounded(capacity);
        Self { tx, rx }
    }
}

pub struct CommandActor {
    handle: Option<thread::JoinHandle<()>>,
    running: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl CommandActor {
    pub fn new<F>(rx: Receiver<ControlCommand>, mut handler: F, core_id: usize, metrics: Option<Arc<crate::metrics::GlobalMetrics>>) -> Self
    where
        F: FnMut(ControlCommand) -> ControlResponse + Send + 'static,
    {
        let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let r = Arc::clone(&running);

        let handle = spawn_pinned(
            "command-actor",
            core_id,
            ThreadPriority::Normal,
            move || {
                while r.load(std::sync::atomic::Ordering::Relaxed) {
                    match rx.recv_timeout(std::time::Duration::from_millis(10)) {
                        Ok(cmd) => {
                            if let Some(ref m) = metrics {
                                m.command_channel_depth.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                            }
                            let _resp = handler(cmd);
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                    }
                }
            },
        );

        Self {
            handle: Some(handle.expect("spawn_pinned failed")),
            running,
        }
    }

    pub fn shutdown(&mut self) {
        self.running.store(false, std::sync::atomic::Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

use std::sync::Arc;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn test_command_channel_send_recv() {
        let channel = CommandChannel::new(100);
        channel
            .tx
            .send(ControlCommand::SetKillSwitch(true))
            .unwrap();
        let cmd = channel.rx.recv().unwrap();
        assert!(matches!(cmd, ControlCommand::SetKillSwitch(true)));
    }

    #[test]
    fn test_command_actor_processes_commands() {
        let channel = CommandChannel::new(100);
        let count = Arc::new(AtomicU64::new(0));
        let c = Arc::clone(&count);

        let mut actor = CommandActor::new(channel.rx, move |cmd| {
            if matches!(cmd, ControlCommand::SetKillSwitch(_)) {
                c.fetch_add(1, Ordering::Relaxed);
            }
            ControlResponse::Ok
        }, 0, None);

        channel
            .tx
            .send(ControlCommand::SetKillSwitch(true))
            .unwrap();
        channel
            .tx
            .send(ControlCommand::SetKillSwitch(false))
            .unwrap();

        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(count.load(Ordering::Relaxed), 2);

        actor.shutdown();
    }
}
