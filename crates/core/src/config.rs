use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;

/// A wrapper type for sensitive strings that masks values in Debug and Serialize output.
/// Use .expose_secret() to access the raw value only when necessary.
#[derive(Clone)]
pub struct SecretString(String);

impl SecretString {
    /// Create a new SecretString from a string.
    pub fn new(value: impl Into<String>) -> Self {
        SecretString(value.into())
    }

    /// Expose the raw secret value. Use sparingly and only where absolutely necessary.
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretString(\"********\")")
    }
}

impl Serialize for SecretString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str("********")
    }
}

impl<'de> Deserialize<'de> for SecretString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer).map(SecretString)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CheckSeverity {
    Veto,
    Advisory,
    Info,
}

impl Default for CheckSeverity {
    fn default() -> Self {
        CheckSeverity::Veto
    }
}

/// Default severity for each built-in risk check.
/// Operators can override these at runtime via config.
pub fn default_check_severities() -> HashMap<String, CheckSeverity> {
    let mut map = HashMap::new();
    map.insert("kill_switch".to_string(), CheckSeverity::Veto);
    map.insert("idempotency".to_string(), CheckSeverity::Veto);
    map.insert("staleness".to_string(), CheckSeverity::Veto);
    map.insert("order_rate".to_string(), CheckSeverity::Veto);
    map.insert("position_limit".to_string(), CheckSeverity::Veto);
    map.insert("portfolio_exposure".to_string(), CheckSeverity::Veto);
    map.insert("leverage".to_string(), CheckSeverity::Veto);
    map.insert("drawdown".to_string(), CheckSeverity::Veto);
    map.insert("volatility".to_string(), CheckSeverity::Advisory);
    map.insert("spread".to_string(), CheckSeverity::Advisory);
    map
}

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
    pub execution_defaults: ExecutionDefaults,
    pub circuit_breaker_config: CircuitBreakerConfig,
    pub channel_config: ChannelConfig,
    pub reactor_config: ReactorConfig,
    pub validator_config: ValidatorConfig,
    pub api_key: String,
    pub control_plane_rate_limit: u32,
    pub safe_mode: bool,
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
    pub risk_intent_staleness_ns: u64,
    pub portfolio_flat_threshold: f64,
    pub initial_equity: f64,
    pub severity_overrides: std::collections::HashMap<String, CheckSeverity>,
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
    pub trade_intent_ttl_ns: u64,
    pub max_long_units: f64,
    pub max_short_units: f64,
    pub urgency_aggressive_threshold: f64,
    pub urgency_normal_threshold: f64,
    pub allow_short: bool,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerConfig {
    pub broker_type: String,
    pub api_key: SecretString,
    pub api_secret: SecretString,
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
    pub volume_ratio_clamp: f64,
    pub regime_volatile_atr_threshold: f64,
    pub regime_strength_atr_divisor: f64,
    pub regime_trending_threshold: f64,
    pub price_window_size: usize,
    pub volume_window_size: usize,
    pub spread_window_size: usize,
    pub return_1_window: usize,
    pub return_5_window: usize,
    pub return_20_window: usize,
    pub feature_capacity: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelConfig {
    pub model_dir: String,
    pub inference_threads: usize,
    pub max_inference_latency_ms: u64,
    pub feature_vector_size: usize,
    pub action_score_rsi_weight: f64,
    pub action_score_macd_weight: f64,
    pub action_score_volatility_weight: f64,
    pub atr_penalty_threshold: f64,
    pub atr_penalty_value: f64,
    pub rsi_overbought: f64,
    pub rsi_oversold: f64,
    pub rsi_neutral: f64,
    pub forecast_trend_weight: f64,
    pub forecast_momentum_weight: f64,
    pub forecast_volume_weight: f64,
    pub confidence_rsi_weight: f64,
    pub confidence_macd_weight: f64,
    pub confidence_regime_weight: f64,
    pub volume_confirmation_threshold: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalConfig {
    pub journal_dir: String,
    pub flush_interval_ms: u64,
    pub snapshot_interval_sec: u64,
    pub max_file_size_mb: u64,
    pub retention_hours: u32,
    pub max_size_mb: u64,
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
    #[serde(default = "default_tick_processing_budget_us")]
    pub tick_processing_budget_us: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionDefaults {
    pub default_order_quantity: f64,
    pub execution_per_symbol_rate_divisor: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    pub failure_threshold: u64,
    pub cooldown_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackpressurePolicy {
    DropOldest,
    DropNewest,
    BlockWithTimeoutMs(u64),
    PrioritizeCritical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelConfig {
    pub per_asset_tick_channel_capacity: usize,
    pub feature_channel_capacity: usize,
    pub risk_channel_capacity: usize,
    pub decision_channel_capacity: usize,
    pub lifecycle_channel_capacity: usize,
    pub command_channel_capacity: usize,
    pub journal_channel_capacity: usize,
    pub per_asset_tick_backpressure_policy: BackpressurePolicy,
    pub feature_backpressure_policy: BackpressurePolicy,
    pub risk_backpressure_policy: BackpressurePolicy,
    pub decision_backpressure_policy: BackpressurePolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReactorConfig {
    pub max_batch_size: usize,
    pub control_batch_size: usize,
    pub sleep_on_empty_us: u64,
    pub backpressure_log_interval: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorConfig {
    pub max_symbol_length: usize,
    pub max_quantity: f64,
    pub max_order_id_length: usize,
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
            execution_defaults: ExecutionDefaults::default(),
            circuit_breaker_config: CircuitBreakerConfig::default(),
            channel_config: ChannelConfig::default(),
            reactor_config: ReactorConfig::default(),
            validator_config: ValidatorConfig::default(),
            api_key: String::new(),
            control_plane_rate_limit: 10,
            safe_mode: false,
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
            risk_intent_staleness_ns: 2_000_000_000,
            portfolio_flat_threshold: 0.001,
            initial_equity: 100_000.0,
            severity_overrides: default_check_severities(),
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
        }
    }
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            broker_type: "alpaca".to_string(),
            api_key: SecretString::new(""),
            api_secret: SecretString::new(""),
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
            volume_ratio_clamp: 0.3,
            regime_volatile_atr_threshold: 0.02,
            regime_strength_atr_divisor: 0.05,
            regime_trending_threshold: 0.5,
            price_window_size: 50,
            volume_window_size: 20,
            spread_window_size: 20,
            return_1_window: 1,
            return_5_window: 5,
            return_20_window: 20,
            feature_capacity: 20,
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
            action_score_rsi_weight: 0.4,
            action_score_macd_weight: 0.4,
            action_score_volatility_weight: 0.2,
            atr_penalty_threshold: 2.0,
            atr_penalty_value: -0.2,
            rsi_overbought: 70.0,
            rsi_oversold: 30.0,
            rsi_neutral: 50.0,
            forecast_trend_weight: 1.0,
            forecast_momentum_weight: 0.3,
            forecast_volume_weight: 0.2,
            confidence_rsi_weight: 0.3,
            confidence_macd_weight: 0.4,
            confidence_regime_weight: 0.3,
            volume_confirmation_threshold: 1.2,
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
            retention_hours: 168,
            max_size_mb: 10_000,
        }
    }
}

fn default_tick_processing_budget_us() -> u64 {
    500
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
            tick_processing_budget_us: default_tick_processing_budget_us(),
        }
    }
}

impl Default for ExecutionDefaults {
    fn default() -> Self {
        Self {
            default_order_quantity: 1.0,
            execution_per_symbol_rate_divisor: 2.0,
        }
    }
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            cooldown_ms: 30_000,
        }
    }
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            per_asset_tick_channel_capacity: 10_000,
            feature_channel_capacity: 1_000,
            risk_channel_capacity: 1_000,
            decision_channel_capacity: 1_000,
            lifecycle_channel_capacity: 1_000,
            command_channel_capacity: 1_000,
            journal_channel_capacity: 10_000,
            per_asset_tick_backpressure_policy: BackpressurePolicy::DropOldest,
            feature_backpressure_policy: BackpressurePolicy::DropNewest,
            risk_backpressure_policy: BackpressurePolicy::DropNewest,
            decision_backpressure_policy: BackpressurePolicy::BlockWithTimeoutMs(10),
        }
    }
}

impl Default for ReactorConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 64,
            control_batch_size: 16,
            sleep_on_empty_us: 10,
            backpressure_log_interval: 1000,
        }
    }
}

impl Default for ValidatorConfig {
    fn default() -> Self {
        Self {
            max_symbol_length: 20,
            max_quantity: 1_000_000.0,
            max_order_id_length: 100,
        }
    }
}

// ── Config validation ────────────────────────────────────────────────

/// A single config-validation violation.
#[derive(Debug, Clone)]
pub struct ConfigViolation {
    pub field: String,
    pub message: String,
}

impl std::fmt::Display for ConfigViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.field, self.message)
    }
}

/// Aggregated error returned when `EngineConfig::validate()` detects one or
/// more problems that would make the configuration unsafe or inconsistent.
#[derive(Debug, Clone)]
pub struct ConfigValidationError {
    pub violations: Vec<ConfigViolation>,
}

impl ConfigValidationError {
    pub fn new(violations: Vec<ConfigViolation>) -> Self {
        Self { violations }
    }

    /// Returns `true` when **no** violations were collected.
    pub fn is_empty(&self) -> bool {
        self.violations.is_empty()
    }
}

impl std::fmt::Display for ConfigValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.violations.is_empty() {
            return Ok(());
        }
        writeln!(f, "config validation failed ({} violation(s)):", self.violations.len())?;
        for v in &self.violations {
            writeln!(f, "  • {}", v)?;
        }
        Ok(())
    }
}

impl std::error::Error for ConfigValidationError {}

/// Helper used inside `validate()` to push a violation into the list.
fn check(cond: bool, field: &str, msg: &str, out: &mut Vec<ConfigViolation>) {
    if !cond {
        out.push(ConfigViolation {
            field: field.to_string(),
            message: msg.to_string(),
        });
    }
}

impl EngineConfig {
    /// Validate internal consistency and safety constraints of this config.
    ///
    /// Returns `Ok(())` when every check passes, or an error listing all
    /// violations so the operator can fix them in one pass.
    pub fn validate(&self) -> Result<(), ConfigValidationError> {
        let mut v = Vec::new();

        // ── Risk-level checks ──────────────────────────────────────
        check(
            self.risk_config.max_leverage > 0.0,
            "risk_config.max_leverage",
            "must be positive (got negative or zero leverage)",
            &mut v,
        );
        check(
            self.risk_config.max_position_per_symbol > 0.0,
            "risk_config.max_position_per_symbol",
            "must be positive",
            &mut v,
        );
        check(
            self.risk_config.max_portfolio_exposure > 0.0,
            "risk_config.max_portfolio_exposure",
            "must be positive",
            &mut v,
        );
        check(
            self.risk_config.max_drawdown_pct >= 0.0 && self.risk_config.max_drawdown_pct <= 100.0,
            "risk_config.max_drawdown_pct",
            "must be in [0, 100]",
            &mut v,
        );
        // Sanity: per-symbol positions shouldn't wildly exceed total exposure
        let implied_max = self.risk_config.max_position_per_symbol * self.asset_configs.len() as f64;
        check(
            implied_max <= self.risk_config.max_portfolio_exposure * 10.0,
            "risk_config.max_position_per_symbol",
            &format!(
                "per-symbol position ({}) × asset count exceeds 10× portfolio exposure ({})",
                implied_max,
                self.risk_config.max_portfolio_exposure
            ),
            &mut v,
        );

        // ── Asset-config checks ────────────────────────────────────
        for (i, a) in self.asset_configs.iter().enumerate() {
            let prefix = format!("asset_configs[{}].{}", i, a.symbol);
            check(
                a.max_position >= 0.0,
                &format!("{}.max_position", prefix),
                "must be non-negative",
                &mut v,
            );
            check(
                a.tick_size > 0.0,
                &format!("{}.tick_size", prefix),
                "must be positive",
                &mut v,
            );
            check(
                !a.symbol.is_empty(),
                &format!("{}.symbol", prefix),
                "must not be empty",
                &mut v,
            );
        }

        // ── Structural consistency ─────────────────────────────────
        check(
            self.max_assets >= self.asset_configs.len(),
            "max_assets",
            &format!(
                "is {} but only {} asset_configs provided (max_assets must be ≥ count)",
                self.max_assets,
                self.asset_configs.len()
            ),
            &mut v,
        );

        // ── Strategy confidence / threshold checks ─────────────────
        check(
            (0.0..=1.0).contains(&self.strategy_config.confidence_minimum),
            "strategy_config.confidence_minimum",
            "must be in [0, 1]",
            &mut v,
        );
        check(
            (0.0..=1.0).contains(&self.strategy_config.long_entry_threshold)
                || self.strategy_config.long_entry_threshold <= 0.0,
            "strategy_config.long_entry_threshold",
            "must be in [-1, 1] range",
            &mut v,
        );
        check(
            (0.0..=1.0).contains(&self.strategy_config.volume_ratio_clamp),
            "strategy_config.volume_ratio_clamp",
            "must be in [0, 1]",
            &mut v,
        );

        // ── Channel capacity sanity ────────────────────────────────
        check(
            self.channel_config.per_asset_tick_channel_capacity > 0,
            "channel_config.per_asset_tick_channel_capacity",
            "must be > 0",
            &mut v,
        );
        check(
            self.channel_config.feature_channel_capacity > 0,
            "channel_config.feature_channel_capacity",
            "must be > 0",
            &mut v,
        );
        check(
            self.channel_config.risk_channel_capacity > 0,
            "channel_config.risk_channel_capacity",
            "must be > 0",
            &mut v,
        );
        check(
            self.channel_config.decision_channel_capacity > 0,
            "channel_config.decision_channel_capacity",
            "must be > 0",
            &mut v,
        );

        // ── Execution defaults ─────────────────────────────────────
        check(
            self.execution_defaults.default_order_quantity > 0.0,
            "execution_defaults.default_order_quantity",
            "must be positive",
            &mut v,
        );
        check(
            self.execution_defaults.execution_per_symbol_rate_divisor > 0.0,
            "execution_defaults.execution_per_symbol_rate_divisor",
            "must be positive",
            &mut v,
        );

        // ── Threading / reactor ─────────────────────────────────────
        check(
            self.reactor_config.max_batch_size > 0,
            "reactor_config.max_batch_size",
            "must be > 0",
            &mut v,
        );
        check(
            self.threading_config.tick_processing_budget_us > 0,
            "threading_config.tick_processing_budget_us",
            "must be > 0",
            &mut v,
        );

        if v.is_empty() {
            Ok(())
        } else {
            Err(ConfigValidationError::new(v))
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

    #[test]
    fn test_secret_string_debug_masking() {
        let secret = SecretString::new("my-secret-key");
        let debug_output = format!("{:?}", secret);
        assert_eq!(debug_output, "SecretString(\"********\")");
        assert!(!debug_output.contains("my-secret-key"));
    }

    #[test]
    fn test_secret_string_expose_secret() {
        let secret = SecretString::new("my-secret-key");
        assert_eq!(secret.expose_secret(), "my-secret-key");
    }

    #[test]
    fn test_secret_string_serialization_masking() {
        let secret = SecretString::new("my-secret-key");
        let json = serde_json::to_string(&secret).unwrap();
        assert_eq!(json, "\"********\"");
        assert!(!json.contains("my-secret-key"));
    }

    #[test]
    fn test_broker_config_debug_masking() {
        let broker = BrokerConfig {
            broker_type: "alpaca".to_string(),
            api_key: SecretString::new("test-key-123"),
            api_secret: SecretString::new("test-secret-456"),
            paper_trading: true,
            ws_url: "wss://example.com".to_string(),
            rest_url: "https://example.com".to_string(),
            max_retries: 3,
            retry_backoff_ms: 1000,
        };
        let debug_output = format!("{:?}", broker);
        assert!(!debug_output.contains("test-key-123"));
        assert!(!debug_output.contains("test-secret-456"));
        assert!(debug_output.contains("********"));
    }

    #[test]
    fn test_broker_config_serialization_masking() {
        let broker = BrokerConfig {
            broker_type: "alpaca".to_string(),
            api_key: SecretString::new("test-key-123"),
            api_secret: SecretString::new("test-secret-456"),
            paper_trading: true,
            ws_url: "wss://example.com".to_string(),
            rest_url: "https://example.com".to_string(),
            max_retries: 3,
            retry_backoff_ms: 1000,
        };
        let json = serde_json::to_string(&broker).unwrap();
        assert!(!json.contains("test-key-123"));
        assert!(!json.contains("test-secret-456"));
        // The serialized form should contain the masked secret
        assert!(json.contains("\"**"));
    }

    // ── Validation tests ───────────────────────────────────────────

    #[test]
    fn test_validate_default_config_passes() {
        let cfg = EngineConfig::default();
        assert!(cfg.validate().is_ok(), "default config should pass validation");
    }

    #[test]
    fn test_validate_rejects_negative_leverage() {
        let mut cfg = EngineConfig::default();
        cfg.risk_config.max_leverage = -1.0;
        let err = cfg.validate().unwrap_err();
        let msgs = format!("{}", err);
        assert!(msgs.contains("leverage"), "error should mention leverage, got: {}", msgs);
    }

    #[test]
    fn test_validate_rejects_zero_leverage() {
        let mut cfg = EngineConfig::default();
        cfg.risk_config.max_leverage = 0.0;
        let err = cfg.validate().unwrap_err();
        assert!(err.violations.iter().any(|v| v.field.contains("leverage")));
    }

    #[test]
    fn test_validate_rejects_inconsistent_asset_count() {
        let mut cfg = EngineConfig::default();
        cfg.max_assets = 0; // but we have 2 asset_configs
        let err = cfg.validate().unwrap_err();
        assert!(
            err.violations.iter().any(|v| v.field == "max_assets"),
            "should flag max_assets mismatch, got: {:?}",
            err.violations
        );
    }

    #[test]
    fn test_validate_rejects_negative_position() {
        let mut cfg = EngineConfig::default();
        cfg.asset_configs[0].max_position = -50.0;
        let err = cfg.validate().unwrap_err();
        assert!(
            err.violations.iter().any(|v| v.field.contains("max_position")),
            "should flag negative max_position, got: {:?}",
            err.violations
        );
    }

    #[test]
    fn test_validate_rejects_zero_tick_size() {
        let mut cfg = EngineConfig::default();
        cfg.asset_configs[1].tick_size = 0.0;
        let err = cfg.validate().unwrap_err();
        assert!(
            err.violations.iter().any(|v| v.field.contains("tick_size")),
            "should flag zero tick_size, got: {:?}",
            err.violations
        );
    }

    #[test]
    fn test_validate_rejects_drawdown_out_of_range() {
        let mut cfg = EngineConfig::default();
        cfg.risk_config.max_drawdown_pct = 150.0; // > 100
        let err = cfg.validate().unwrap_err();
        assert!(
            err.violations.iter().any(|v| v.field.contains("drawdown")),
            "should flag out-of-range drawdown, got: {:?}",
            err.violations
        );
    }

    #[test]
    fn test_validate_rejects_confidence_out_of_range() {
        let mut cfg = EngineConfig::default();
        cfg.strategy_config.confidence_minimum = 2.0; // > 1
        let err = cfg.validate().unwrap_err();
        assert!(
            err.violations.iter().any(|v| v.field.contains("confidence_minimum")),
            "should flag out-of-range confidence, got: {:?}",
            err.violations
        );
    }

    #[test]
    fn test_validate_multiple_violations_reported_at_once() {
        let mut cfg = EngineConfig::default();
        cfg.risk_config.max_leverage = -5.0;
        cfg.risk_config.max_portfolio_exposure = -1.0;
        cfg.asset_configs[0].tick_size = 0.0;
        cfg.channel_config.feature_channel_capacity = 0;
        let err = cfg.validate().unwrap_err();
        // Should collect all violations, not just the first one
        assert!(err.violations.len() >= 3, "expected ≥3 violations, got {}: {:?}", err.violations.len(), err.violations);
    }

    #[test]
    fn test_validation_error_display_format() {
        let violations = vec![
            ConfigViolation { field: "foo".into(), message: "bad value".into() },
            ConfigViolation { field: "bar".into(), message: "must be positive".into() },
        ];
        let err = ConfigValidationError::new(violations);
        let display = format!("{}", err);
        assert!(display.contains("2 violation(s)"));
        assert!(display.contains("foo"));
        assert!(display.contains("bad value"));
        assert!(display.contains("bar"));
    }
}
