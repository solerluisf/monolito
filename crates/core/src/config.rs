use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    pub max_assets: usize,
    pub asset_configs: Vec<AssetConfig>,
    pub risk_config: RiskConfig,
    pub strategy_config: StrategyConfig,
    pub broker_config: BrokerConfig,
    pub feature_config: FeatureConfig,
    pub model_config: ModelConfig,
    pub journal_config: JournalConfig,
    pub threading_config: ThreadingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetConfig {
    pub symbol: String,
    pub enabled: bool,
    pub max_position: f64,
    pub tick_size: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    pub long_entry_threshold: f64,
    pub short_entry_threshold: f64,
    pub exit_threshold: f64,
    pub confidence_minimum: f64,
    pub hysteresis_deadband: f64,
    pub entry_cooldown_ms: u64,
    pub exit_cooldown_ms: u64,
    pub prediction_staleness_ns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerConfig {
    pub broker_type: String,
    pub api_key: String,
    pub api_secret: String,
    pub paper_trading: bool,
    pub ws_url: String,
    pub rest_url: String,
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureConfig {
    pub rsi_period: usize,
    pub macd_fast: usize,
    pub macd_slow: usize,
    pub macd_signal: usize,
    pub atr_period: usize,
    pub ema_periods: Vec<usize>,
    pub rolling_window_sizes: Vec<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelConfig {
    pub model_dir: String,
    pub inference_threads: usize,
    pub max_inference_latency_ms: u64,
    pub feature_vector_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalConfig {
    pub journal_dir: String,
    pub flush_interval_ms: u64,
    pub snapshot_interval_sec: u64,
    pub max_file_size_mb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadingConfig {
    pub prediction_core_id: usize,
    pub risk_core_id: usize,
    pub journal_core_id: usize,
    pub alpaca_feed_core_id: usize,
    pub tick_reactor_core_id: usize,
    pub execution_core_id: usize,
    pub heartbeat_core_id: usize,
    pub command_core_id: usize,
    pub heartbeat_timeout_ns: u64,
    pub heartbeat_check_interval_ms: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            max_assets: 2,
            asset_configs: vec![
                AssetConfig {
                    symbol: "AAPL".to_string(),
                    enabled: true,
                    max_position: 100.0,
                    tick_size: 0.01,
                },
                AssetConfig {
                    symbol: "MSFT".to_string(),
                    enabled: true,
                    max_position: 100.0,
                    tick_size: 0.01,
                },
            ],
            risk_config: RiskConfig::default(),
            strategy_config: StrategyConfig::default(),
            broker_config: BrokerConfig::default(),
            feature_config: FeatureConfig::default(),
            model_config: ModelConfig::default(),
            journal_config: JournalConfig::default(),
            threading_config: ThreadingConfig::default(),
        }
    }
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_portfolio_exposure: 1_000_000.0,
            max_leverage: 4.0,
            max_drawdown_pct: 5.0,
            max_order_rate_per_sec: 10,
            max_position_per_symbol: 50_000.0,
            max_volatility: 0.05,
            max_spread_bps: 50.0,
            max_slippage_bps: 10.0,
            allow_short: true,
            kill_switch_on_drawdown: true,
        }
    }
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            long_entry_threshold: 0.6,
            short_entry_threshold: -0.6,
            exit_threshold: 0.1,
            confidence_minimum: 0.5,
            hysteresis_deadband: 0.15,
            entry_cooldown_ms: 5000,
            exit_cooldown_ms: 2000,
            prediction_staleness_ns: 150_000_000, // 150ms
        }
    }
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            broker_type: "alpaca".to_string(),
            api_key: String::new(),
            api_secret: String::new(),
            paper_trading: true,
            ws_url: "wss://stream.data.alpaca.markets/v2/iex".to_string(),
            rest_url: "https://paper-api.alpaca.markets".to_string(),
            max_retries: 3,
            retry_backoff_ms: 1000,
        }
    }
}

impl Default for FeatureConfig {
    fn default() -> Self {
        Self {
            rsi_period: 14,
            macd_fast: 12,
            macd_slow: 26,
            macd_signal: 9,
            atr_period: 14,
            ema_periods: vec![9, 21, 50],
            rolling_window_sizes: vec![5, 20],
        }
    }
}

impl Default for ModelConfig {
    fn default() -> Self {
        Self {
            model_dir: "models".to_string(),
            inference_threads: 1,
            max_inference_latency_ms: 5,
            feature_vector_size: 128,
        }
    }
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self {
            journal_dir: "journal".to_string(),
            flush_interval_ms: 100,
            snapshot_interval_sec: 60,
            max_file_size_mb: 100,
        }
    }
}

impl Default for ThreadingConfig {
    fn default() -> Self {
        Self {
            prediction_core_id: 3,
            risk_core_id: 2,
            journal_core_id: 0,
            alpaca_feed_core_id: 0,
            tick_reactor_core_id: 0,
            execution_core_id: 0,
            heartbeat_core_id: 0,
            command_core_id: 0,
            heartbeat_timeout_ns: 2_000_000_000, // 2 seconds
            heartbeat_check_interval_ms: 500,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = EngineConfig::default();
        assert_eq!(config.max_assets, 2);
        assert_eq!(config.asset_configs.len(), 2);
        assert_eq!(config.asset_configs[0].symbol, "AAPL");
    }

    #[test]
    fn test_config_serialization() {
        let config = EngineConfig::default();
        let serialized = toml::to_string(&config).unwrap();
        let deserialized: EngineConfig = toml::from_str(&serialized).unwrap();
        assert_eq!(deserialized.max_assets, config.max_assets);
    }

    #[test]
    fn test_risk_config_defaults() {
        let risk = RiskConfig::default();
        assert_eq!(risk.max_drawdown_pct, 5.0);
        assert!(risk.allow_short);
        assert!(risk.kill_switch_on_drawdown);
    }

    #[test]
    fn test_strategy_config_defaults() {
        let strat = StrategyConfig::default();
        assert_eq!(strat.prediction_staleness_ns, 150_000_000);
        assert_eq!(strat.confidence_minimum, 0.5);
    }
}
