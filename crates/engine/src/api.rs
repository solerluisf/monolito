use axum::{
    extract::State,
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{Json, Response, IntoResponse},
    routing::{get, post, put},
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::{Duration, Instant};

use unified_trading_core::KillSwitch;
use unified_trading_core::GlobalMetrics;
use unified_trading_core::MetricsSnapshot;
use unified_trading_core::{
    ControlCommand, StrategyParams, RiskConfigUpdate, BrokerConfigUpdate,
    FeatureConfigUpdate, ModelConfigUpdate, JournalConfigUpdate, AssetConfigUpdate,
    ExecutionDefaultsUpdate, CircuitBreakerConfigUpdate, RateLimitConfigUpdate,
    ChannelConfigUpdate, ReactorConfigUpdate, ValidatorConfigUpdate,
};
use unified_trading_core::PositionManager;
use unified_trading_core::EngineConfig;
use parking_lot::RwLock;

use execution::{OrderTracker, RateLimiter};
use gateway::CircuitBreaker;
use model::ModelRegistry;

#[derive(Clone)]
pub struct SimpleRateLimiter {
    inner: Arc<Mutex<HashMap<String, (Instant, u32)>>>,
    max_requests: u32,
    window_secs: u64,
}

impl SimpleRateLimiter {
    pub fn new(max_requests: u32, window_secs: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            max_requests,
            window_secs,
        }
    }

    pub fn check(&self, key: &str) -> bool {
        let now = Instant::now();
        let window = Duration::from_secs(self.window_secs);
        let mut map = self.inner.lock().unwrap();
        let entry = map.entry(key.to_string()).or_insert((now, 0));

        if now.duration_since(entry.0) > window {
            *entry = (now, 1);
            true
        } else if entry.1 >= self.max_requests {
            false
        } else {
            entry.1 += 1;
            true
        }
    }
}

#[derive(Clone)]
pub struct ApiState {
    pub kill_switch: Arc<KillSwitch>,
    pub metrics: Arc<GlobalMetrics>,
    pub command_tx: crossbeam_channel::Sender<ControlCommand>,
    pub config: Arc<RwLock<EngineConfig>>,
    pub position_manager: Arc<PositionManager>,
    pub heartbeats: Option<Arc<parking_lot::RwLock<HashMap<String, Arc<std::sync::atomic::AtomicU64>>>>>,
    pub execution_states: Arc<std::sync::Mutex<HashMap<String, crate::engine::ExecutionSharedState>>>,
    pub strategy_registry: Arc<std::sync::Mutex<HashMap<String, crate::engine::StrategySwapRef>>>,
    pub model_registry: Arc<ModelRegistry>,
    pub api_key: String,
    pub rate_limiter: SimpleRateLimiter,
}

// ------------------------------------------------------------------
// Request / Response structs
// ------------------------------------------------------------------

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub kill_switch_active: bool,
    pub uptime: String,
}

#[derive(Serialize)]
pub struct KillSwitchResponse {
    pub active: bool,
    pub activated_at_ns: u64,
    pub activation_count: u64,
    pub open_orders: Vec<String>,
}

#[derive(Deserialize)]
pub struct KillSwitchRequest {
    pub active: bool,
}

#[derive(Serialize)]
pub struct MetricsResponse {
    pub metrics: MetricsSnapshot,
}

#[derive(Deserialize)]
pub struct StrategyParamsRequest {
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

#[derive(Deserialize)]
pub struct StrategySwapRequest {
    pub symbol: String,
    pub strategy_type: String,
    pub params: Option<StrategyParamsRequest>,
}

#[derive(Serialize)]
pub struct StrategySwapResponse {
    pub status: String,
    pub symbol: String,
    pub strategy_type: String,
}

#[derive(Serialize)]
pub struct StatusResponse {
    pub kill_switch: KillSwitchResponse,
    pub metrics: MetricsSnapshot,
}

#[derive(Serialize)]
pub struct CircuitBreakerResponse {
    pub is_open: bool,
    pub failure_count: u64,
    pub success_count: u64,
    pub last_failure_ns: u64,
    pub state: String,
}

#[derive(Serialize)]
pub struct CircuitBreakerStatusResponse {
    pub symbol: String,
    pub state: String,
    pub is_open: bool,
    pub failure_count: u64,
}

#[derive(Deserialize)]
pub struct AssetControlRequest {
    pub symbol: String,
}

#[derive(Deserialize)]
pub struct AssetConfigRequest {
    pub enabled: bool,
    pub max_position: f64,
    pub tick_size: f64,
}

#[derive(Serialize)]
pub struct AssetConfigResponse {
    pub symbol: String,
    pub enabled: bool,
    pub max_position: f64,
    pub tick_size: f64,
}

#[derive(Deserialize)]
pub struct ModeRequest {
    pub mode: String,
}

#[derive(Deserialize)]
pub struct PaperTradingRequest {
    pub paper_trading: bool,
}

#[derive(Deserialize)]
pub struct RiskParamsRequest {
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

#[derive(Serialize)]
pub struct RiskParamsResponse {
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

#[derive(Deserialize)]
pub struct BrokerParamsRequest {
    pub broker_type: String,
    pub paper_trading: bool,
    pub ws_url: String,
    pub rest_url: String,
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
}

#[derive(Serialize)]
pub struct BrokerParamsResponse {
    pub broker_type: String,
    pub paper_trading: bool,
    pub ws_url: String,
    pub rest_url: String,
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
}

#[derive(Deserialize)]
pub struct FeatureParamsRequest {
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

#[derive(Serialize)]
pub struct FeatureParamsResponse {
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

#[derive(Deserialize)]
pub struct ModelParamsRequest {
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

#[derive(Serialize)]
pub struct ModelParamsResponse {
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

#[derive(Deserialize)]
pub struct JournalParamsRequest {
    pub journal_dir: String,
    pub flush_interval_ms: u64,
    pub snapshot_interval_sec: u64,
    pub max_file_size_mb: u64,
}

#[derive(Serialize)]
pub struct JournalParamsResponse {
    pub journal_dir: String,
    pub flush_interval_ms: u64,
    pub snapshot_interval_sec: u64,
    pub max_file_size_mb: u64,
}

#[derive(Deserialize)]
pub struct ExecutionDefaultsRequest {
    pub default_order_quantity: f64,
    pub execution_per_symbol_rate_divisor: f64,
}

#[derive(Serialize)]
pub struct ExecutionDefaultsResponse {
    pub default_order_quantity: f64,
    pub execution_per_symbol_rate_divisor: f64,
}

#[derive(Deserialize)]
pub struct RateLimitRequest {
    pub global_rate: f64,
    pub per_symbol_rate: f64,
}

#[derive(Serialize)]
pub struct RateLimitResponse {
    pub global_rate: f64,
    pub per_symbol_rate: f64,
}

#[derive(Deserialize)]
pub struct CircuitBreakerConfigRequest {
    pub failure_threshold: u64,
    pub cooldown_ms: u64,
}

#[derive(Serialize)]
pub struct CircuitBreakerConfigResponse {
    pub failure_threshold: u64,
    pub cooldown_ms: u64,
}

#[derive(Deserialize)]
pub struct SwapModelRequest {
    pub model_id: String,
}

#[derive(Serialize)]
pub struct ActiveModelResponse {
    pub model_id: String,
    pub version: u32,
    pub input_features: Vec<String>,
    pub applicable_regimes: Vec<i32>,
    pub priority: u32,
}

#[derive(Serialize)]
pub struct ModelListResponse {
    pub models: Vec<ActiveModelResponse>,
}

#[derive(Deserialize)]
pub struct ChannelConfigRequest {
    pub per_asset_tick_channel_capacity: usize,
    pub feature_channel_capacity: usize,
    pub risk_channel_capacity: usize,
    pub decision_channel_capacity: usize,
    pub lifecycle_channel_capacity: usize,
    pub command_channel_capacity: usize,
    pub journal_channel_capacity: usize,
}

#[derive(Serialize)]
pub struct ChannelConfigResponse {
    pub per_asset_tick_channel_capacity: usize,
    pub feature_channel_capacity: usize,
    pub risk_channel_capacity: usize,
    pub decision_channel_capacity: usize,
    pub lifecycle_channel_capacity: usize,
    pub command_channel_capacity: usize,
    pub journal_channel_capacity: usize,
}

#[derive(Deserialize)]
pub struct ReactorConfigRequest {
    pub max_batch_size: usize,
    pub control_batch_size: usize,
    pub sleep_on_empty_us: u64,
    pub backpressure_log_interval: u64,
}

#[derive(Serialize)]
pub struct ReactorConfigResponse {
    pub max_batch_size: usize,
    pub control_batch_size: usize,
    pub sleep_on_empty_us: u64,
    pub backpressure_log_interval: u64,
}

#[derive(Deserialize)]
pub struct ValidatorConfigRequest {
    pub max_symbol_length: usize,
    pub max_quantity: f64,
    pub max_order_id_length: usize,
}

#[derive(Serialize)]
pub struct ValidatorConfigResponse {
    pub max_symbol_length: usize,
    pub max_quantity: f64,
    pub max_order_id_length: usize,
}

#[derive(Serialize)]
pub struct PortfolioResponse {
    pub positions: Vec<unified_trading_core::position_manager::Position>,
    pub total_unrealized_pnl: f64,
    pub total_realized_pnl: f64,
    pub total_market_value: f64,
    pub position_count: usize,
}

#[derive(Serialize)]
pub struct OrdersResponse {
    pub orders: Vec<execution::order_tracker::Order>,
}

#[derive(Serialize)]
pub struct TrackedOrdersResponse {
    pub open_orders: Vec<String>,
}

#[derive(Serialize)]
pub struct IdempotencyStatusResponse {
    pub store_size: usize,
    pub store_capacity: usize,
}

#[derive(Serialize)]
pub struct HeartbeatDetail {
    pub thread: String,
    pub last_timestamp_ns: u64,
}

#[derive(Serialize)]
pub struct HeartbeatsResponse {
    pub status: String,
    pub threads: Vec<HeartbeatDetail>,
}

#[derive(Serialize)]
pub struct RateLimiterStatusResponse {
    pub symbol: String,
    pub global_tokens: f64,
    pub symbol_tokens: f64,
    pub symbol_percent_remaining: f64,
}

// ------------------------------------------------------------------
// Middleware
// ------------------------------------------------------------------

async fn auth_middleware(
    State(state): State<ApiState>,
    req: Request,
    next: Next,
) -> Response {
    let method = req.method().clone();
    if method == axum::http::Method::POST || method == axum::http::Method::PUT || method == axum::http::Method::DELETE {
        if !state.api_key.is_empty() {
            let auth_header = req.headers().get("Authorization")
                .and_then(|h| h.to_str().ok());
            let expected = format!("Bearer {}", state.api_key);
            match auth_header {
                Some(header) if header == expected => {}
                _ => return StatusCode::UNAUTHORIZED.into_response(),
            }
        }
    }
    next.run(req).await
}

async fn rate_limit_middleware(
    State(state): State<ApiState>,
    req: Request,
    next: Next,
) -> Response {
    let method = req.method().clone();
    if method == axum::http::Method::POST || method == axum::http::Method::PUT || method == axum::http::Method::DELETE {
        let ip = req.headers()
            .get("x-forwarded-for")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.split(',').next())
            .unwrap_or("unknown")
            .to_string();
        if !state.rate_limiter.check(&ip) {
            return StatusCode::TOO_MANY_REQUESTS.into_response();
        }
    }
    next.run(req).await
}

// ------------------------------------------------------------------
// Router
// ------------------------------------------------------------------

pub fn create_router(state: ApiState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/health/kill-switch", get(kill_switch_status))
        .route("/health/heartbeats", get(heartbeats_handler))
        .route("/control/kill-switch", post(set_kill_switch))
        .route("/control/circuit-breaker", get(circuit_breaker_status).post(set_circuit_breaker_config_handler).put(set_circuit_breaker_config_handler))
        .route("/control/circuit-breaker/trip", post(trip_circuit_breaker))
        .route("/control/circuit-breaker/reset", post(reset_circuit_breaker))
        .route("/control/circuit-breaker/config", post(set_circuit_breaker_config_handler).put(set_circuit_breaker_config_handler))
        .route("/control/strategy-swap", post(strategy_swap_handler))
        .route("/control/strategy/:symbol/parameters", post(set_strategy_params_handler).put(set_strategy_params_handler))
        .route("/control/asset/:symbol/pause", post(pause_asset_handler))
        .route("/control/asset/:symbol/resume", post(resume_asset_handler))
        .route("/control/asset/:symbol/config", get(get_asset_config_handler).put(set_asset_config_handler))
        .route("/control/mode", post(set_mode_handler))
        .route("/control/broker", put(set_broker_params_handler))
        .route("/control/broker/mode", post(set_paper_trading_handler).put(set_paper_trading_handler))
        .route("/control/risk/parameters", post(set_risk_params_handler).put(set_risk_params_handler))
        .route("/control/risk/parameters", get(get_risk_params_handler))
        .route("/control/features/parameters", post(set_feature_params_handler).put(set_feature_params_handler))
        .route("/control/model/parameters", post(set_model_params_handler).put(set_model_params_handler))
        .route("/control/model/active", get(get_active_model_handler))
        .route("/control/model/list", get(list_models_handler))
        .route("/control/model/swap", post(swap_model_handler))
        .route("/control/journal/config", post(set_journal_params_handler).put(set_journal_params_handler))
        .route("/control/journal/flush", post(flush_journal_handler))
        .route("/control/config/reload", post(reload_config_handler))
        .route("/control/execution/defaults", post(set_execution_defaults_handler).put(set_execution_defaults_handler))
        .route("/control/execution/rate-limits", post(set_rate_limits_handler).put(set_rate_limits_handler))
        .route("/control/channel/config", get(get_channel_params_handler).post(set_channel_params_handler).put(set_channel_params_handler))
        .route("/control/reactor/config", get(get_reactor_params_handler).post(set_reactor_params_handler).put(set_reactor_params_handler))
        .route("/control/validator/config", get(get_validator_params_handler).post(set_validator_params_handler).put(set_validator_params_handler))
        .route("/metrics", get(metrics_handler))
        .route("/metrics/prometheus", get(prometheus_handler))
        .route("/status", get(status_handler))
        .route("/status/circuit-breaker", get(circuit_breaker_status))
        .route("/status/risk/portfolio", get(portfolio_handler))
        .route("/status/risk/idempotency", get(idempotency_status_handler))
        .route("/status/execution/orders", get(orders_handler))
        .route("/status/execution/tracked-orders", get(tracked_orders_handler))
        .route("/status/rate-limiter", get(rate_limiter_status_handler))
        .route("/status/heartbeats", get(heartbeats_handler))
        .route("/status/kill-switch/orders", get(kill_switch_orders_handler))
        .route("/status/per-symbol", get(per_symbol_stats_handler))
        .route("/shutdown", post(shutdown_handler))
        .layer(axum::middleware::from_fn_with_state(state.clone(), rate_limit_middleware))
        .layer(axum::middleware::from_fn_with_state(state.clone(), auth_middleware))
        .with_state(state)
}

// ------------------------------------------------------------------
// Handlers
// ------------------------------------------------------------------

fn send_command(state: &ApiState, cmd: ControlCommand) -> StatusCode {
    state.metrics.command_channel_depth.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    match state.command_tx.send(cmd) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

async fn health_handler(State(state): State<ApiState>) -> Json<HealthResponse> {
    let active = state.kill_switch.is_active();
    Json(HealthResponse {
        status: if active { "stopped" } else { "running" }.to_string(),
        kill_switch_active: active,
        uptime: "unknown".to_string(),
    })
}

async fn kill_switch_status(State(state): State<ApiState>) -> Json<KillSwitchResponse> {
    Json(KillSwitchResponse {
        active: state.kill_switch.is_active(),
        activated_at_ns: state.kill_switch.activated_at_ns(),
        activation_count: state.kill_switch.activation_count(),
        open_orders: state.kill_switch.get_open_orders(),
    })
}

async fn set_kill_switch(
    State(state): State<ApiState>,
    Json(req): Json<KillSwitchRequest>,
) -> StatusCode {
    let cmd = if req.active {
        ControlCommand::SetKillSwitch(true)
    } else {
        ControlCommand::SetKillSwitch(false)
    };
    send_command(&state, cmd)
}

async fn strategy_swap_handler(
    State(state): State<ApiState>,
    Json(req): Json<StrategySwapRequest>,
) -> (StatusCode, Json<StrategySwapResponse>) {
    let params = req.params.map(|p| StrategyParams {
        long_entry_threshold: p.long_entry_threshold,
        short_entry_threshold: p.short_entry_threshold,
        exit_threshold: p.exit_threshold,
        confidence_minimum: p.confidence_minimum,
        hysteresis_deadband: p.hysteresis_deadband,
        entry_cooldown_ms: p.entry_cooldown_ms,
        exit_cooldown_ms: p.exit_cooldown_ms,
        prediction_staleness_ns: p.prediction_staleness_ns,
        allow_short: p.allow_short,
        max_long_units: p.max_long_units,
        max_short_units: p.max_short_units,
        trade_intent_ttl_ns: p.trade_intent_ttl_ns,
        urgency_aggressive_threshold: p.urgency_aggressive_threshold,
        urgency_normal_threshold: p.urgency_normal_threshold,
    });

    let cmd = ControlCommand::SwapStrategy {
        symbol: req.symbol.clone(),
        strategy_type: req.strategy_type.clone(),
        params,
    };
    state.metrics.command_channel_depth.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    match state.command_tx.send(cmd) {
        Ok(_) => (
            StatusCode::OK,
            Json(StrategySwapResponse {
                status: "swap_requested".to_string(),
                symbol: req.symbol,
                strategy_type: req.strategy_type,
            }),
        ),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(StrategySwapResponse {
                status: "failed".to_string(),
                symbol: req.symbol,
                strategy_type: req.strategy_type,
            }),
        ),
    }
}

async fn set_strategy_params_handler(
    State(state): State<ApiState>,
    axum::extract::Path(symbol): axum::extract::Path<String>,
    Json(req): Json<StrategyParamsRequest>,
) -> StatusCode {
    let params = StrategyParams {
        long_entry_threshold: req.long_entry_threshold,
        short_entry_threshold: req.short_entry_threshold,
        exit_threshold: req.exit_threshold,
        confidence_minimum: req.confidence_minimum,
        hysteresis_deadband: req.hysteresis_deadband,
        entry_cooldown_ms: req.entry_cooldown_ms,
        exit_cooldown_ms: req.exit_cooldown_ms,
        prediction_staleness_ns: req.prediction_staleness_ns,
        allow_short: req.allow_short,
        max_long_units: req.max_long_units,
        max_short_units: req.max_short_units,
        trade_intent_ttl_ns: req.trade_intent_ttl_ns,
        urgency_aggressive_threshold: req.urgency_aggressive_threshold,
        urgency_normal_threshold: req.urgency_normal_threshold,
    };
    let cmd = ControlCommand::SwapStrategy {
        symbol,
        strategy_type: "custom".to_string(),
        params: Some(params),
    };
    send_command(&state, cmd)
}

async fn metrics_handler(State(state): State<ApiState>) -> Json<MetricsResponse> {
    Json(MetricsResponse {
        metrics: state.metrics.snapshot(),
    })
}

async fn prometheus_handler(State(state): State<ApiState>) -> Response<String> {
    let snap = state.metrics.snapshot();
    let mut output = String::new();

    output.push_str("# HELP ticks_processed Total number of ticks processed\n");
    output.push_str("# TYPE ticks_processed counter\n");
    output.push_str(&format!("ticks_processed {}\n", snap.ticks_processed));

    output.push_str("# HELP features_computed Total number of feature vectors computed\n");
    output.push_str("# TYPE features_computed counter\n");
    output.push_str(&format!("features_computed {}\n", snap.features_computed));

    output.push_str("# HELP inferences_run Total number of model inferences\n");
    output.push_str("# TYPE inferences_run counter\n");
    output.push_str(&format!("inferences_run {}\n", snap.inferences_run));

    output.push_str("# HELP intents_generated Total trade intents generated\n");
    output.push_str("# TYPE intents_generated counter\n");
    output.push_str(&format!("intents_generated {}\n", snap.intents_generated));

    output.push_str("# HELP intents_approved Total trade intents approved by risk\n");
    output.push_str("# TYPE intents_approved counter\n");
    output.push_str(&format!("intents_approved {}\n", snap.intents_approved));

    output.push_str("# HELP intents_rejected Total trade intents rejected by risk\n");
    output.push_str("# TYPE intents_rejected counter\n");
    output.push_str(&format!("intents_rejected {}\n", snap.intents_rejected));

    output.push_str("# HELP dropped_intents Total intents dropped due to full channel\n");
    output.push_str("# TYPE dropped_intents counter\n");
    output.push_str(&format!("dropped_intents {}\n", snap.dropped_intents));

    output.push_str("# HELP stale_predictions Total predictions rejected due to staleness\n");
    output.push_str("# TYPE stale_predictions counter\n");
    output.push_str(&format!("stale_predictions {}\n", snap.stale_predictions));

    output.push_str("# HELP orders_submitted Total orders submitted to broker\n");
    output.push_str("# TYPE orders_submitted counter\n");
    output.push_str(&format!("orders_submitted {}\n", snap.orders_submitted));

    output.push_str("# HELP orders_filled Total orders filled\n");
    output.push_str("# TYPE orders_filled counter\n");
    output.push_str(&format!("orders_filled {}\n", snap.orders_filled));

    output.push_str("# HELP orders_cancelled Total orders cancelled\n");
    output.push_str("# TYPE orders_cancelled counter\n");
    output.push_str(&format!("orders_cancelled {}\n", snap.orders_cancelled));

    output.push_str("# HELP orders_rejected Total orders rejected by broker or rate limiter\n");
    output.push_str("# TYPE orders_rejected counter\n");
    output.push_str(&format!("orders_rejected {}\n", snap.orders_rejected));

    output.push_str("# HELP circuit_breaker_trips Total circuit breaker activations\n");
    output.push_str("# TYPE circuit_breaker_trips counter\n");
    output.push_str(&format!("circuit_breaker_trips {}\n", snap.circuit_breaker_trips));

    output.push_str("# HELP kill_switch_activations Total kill switch activations\n");
    output.push_str("# TYPE kill_switch_activations counter\n");
    output.push_str(&format!("kill_switch_activations {}\n", snap.kill_switch_activations));

    output.push_str("# HELP config_reloads Total config hot-reloads\n");
    output.push_str("# TYPE config_reloads counter\n");
    output.push_str(&format!("config_reloads {}\n", snap.config_reloads));

    output.push_str("# HELP journal_writes Total journal writes\n");
    output.push_str("# TYPE journal_writes counter\n");
    output.push_str(&format!("journal_writes {}\n", snap.journal_writes));

    output.push_str("# HELP heartbeat_misses Total thread heartbeat misses\n");
    output.push_str("# TYPE heartbeat_misses counter\n");
    output.push_str(&format!("heartbeat_misses {}\n", snap.heartbeat_misses));

    output.push_str("# HELP errors Total errors encountered\n");
    output.push_str("# TYPE errors counter\n");
    output.push_str(&format!("errors {}\n", snap.errors));

    output.push_str("# HELP orders_lifecycle_events Total order lifecycle events\n");
    output.push_str("# TYPE orders_lifecycle_events counter\n");
    output.push_str(&format!("orders_lifecycle_events {}\n", snap.orders_lifecycle_events));

    output.push_str("# HELP tick_to_intent_latency Tick to intent latency histogram\n");
    output.push_str("# TYPE tick_to_intent_latency counter\n");
    for (i, v) in snap.tick_to_intent_latency.iter().enumerate() {
        output.push_str(&format!("tick_to_intent_latency_bucket{{le=\"{}\"}} {}\n", i, v));
    }

    output.push_str("# HELP risk_check_latency Risk check latency histogram\n");
    output.push_str("# TYPE risk_check_latency counter\n");
    for (i, v) in snap.risk_check_latency.iter().enumerate() {
        output.push_str(&format!("risk_check_latency_bucket{{le=\"{}\"}} {}\n", i, v));
    }

    output.push_str("# HELP journal_flush_latency Journal flush latency histogram\n");
    output.push_str("# TYPE journal_flush_latency counter\n");
    for (i, v) in snap.journal_flush_latency.iter().enumerate() {
        output.push_str(&format!("journal_flush_latency_bucket{{le=\"{}\"}} {}\n", i, v));
    }

    output.push_str("# HELP broker_send_latency Broker send latency histogram\n");
    output.push_str("# TYPE broker_send_latency counter\n");
    for (i, v) in snap.broker_send_latency.iter().enumerate() {
        output.push_str(&format!("broker_send_latency_bucket{{le=\"{}\"}} {}\n", i, v));
    }

    output.push_str("# HELP feed_latency Feed latency histogram\n");
    output.push_str("# TYPE feed_latency counter\n");
    for (i, v) in snap.feed_latency.iter().enumerate() {
        output.push_str(&format!("feed_latency_bucket{{le=\"{}\"}} {}\n", i, v));
    }

    output.push_str("# HELP broker_round_trip_latency Broker round-trip latency histogram\n");
    output.push_str("# TYPE broker_round_trip_latency counter\n");
    for (i, v) in snap.broker_round_trip_latency.iter().enumerate() {
        output.push_str(&format!("broker_round_trip_latency_bucket{{le=\"{}\"}} {}\n", i, v));
    }

    output.push_str("# HELP feature_channel_depth Feature channel depth gauge\n");
    output.push_str("# TYPE feature_channel_depth gauge\n");
    output.push_str(&format!("feature_channel_depth {}\n", snap.feature_channel_depth));

    output.push_str("# HELP risk_channel_depth Risk channel depth gauge\n");
    output.push_str("# TYPE risk_channel_depth gauge\n");
    output.push_str(&format!("risk_channel_depth {}\n", snap.risk_channel_depth));

    output.push_str("# HELP decision_channel_depth Decision channel depth gauge\n");
    output.push_str("# TYPE decision_channel_depth gauge\n");
    output.push_str(&format!("decision_channel_depth {}\n", snap.decision_channel_depth));

    output.push_str("# HELP lifecycle_channel_depth Lifecycle channel depth gauge\n");
    output.push_str("# TYPE lifecycle_channel_depth gauge\n");
    output.push_str(&format!("lifecycle_channel_depth {}\n", snap.lifecycle_channel_depth));

    output.push_str("# HELP command_channel_depth Command channel depth gauge\n");
    output.push_str("# TYPE command_channel_depth gauge\n");
    output.push_str(&format!("command_channel_depth {}\n", snap.command_channel_depth));

    output.push_str("# HELP journal_channel_depth Journal channel depth gauge\n");
    output.push_str("# TYPE journal_channel_depth gauge\n");
    output.push_str(&format!("journal_channel_depth {}\n", snap.journal_channel_depth));

    output.push_str("# HELP feed_gaps Total feed gaps detected (>1s between ticks)\n");
    output.push_str("# TYPE feed_gaps counter\n");
    output.push_str(&format!("feed_gaps {}\n", snap.feed_gaps));

    output.push_str("# HELP decision_latency Decision latency histogram (tick timestamp → decision send)\n");
    output.push_str("# TYPE decision_latency counter\n");
    for (i, v) in snap.decision_latency.iter().enumerate() {
        output.push_str(&format!("decision_latency_bucket{{le=\"{}\"}} {}\n", i, v));
    }

    output.push_str("# HELP circuit_breaker_state Circuit breaker state per symbol (1=open, 0=closed)\n");
    output.push_str("# TYPE circuit_breaker_state gauge\n");
    let cb_states = state.execution_states.lock().unwrap();
    for (symbol, exec_state) in cb_states.iter() {
        let is_open = exec_state.circuit_breaker.is_open.load(std::sync::atomic::Ordering::Relaxed);
        output.push_str(&format!("circuit_breaker_state{{symbol=\"{}\"}} {}\n", symbol, if is_open { 1 } else { 0 }));
    }

    for (symbol, count) in &snap.per_symbol_ticks {
        output.push_str("# HELP per_symbol_ticks Ticks processed per symbol\n");
        output.push_str("# TYPE per_symbol_ticks counter\n");
        output.push_str(&format!("per_symbol_ticks{{symbol=\"{}\"}} {}\n", symbol, count));
    }

    for (symbol, count) in &snap.per_symbol_features {
        output.push_str("# HELP per_symbol_features Features computed per symbol\n");
        output.push_str("# TYPE per_symbol_features counter\n");
        output.push_str(&format!("per_symbol_features{{symbol=\"{}\"}} {}\n", symbol, count));
    }

    for (symbol, count) in &snap.per_symbol_intents_approved {
        output.push_str("# HELP per_symbol_intents_approved Intents approved per symbol\n");
        output.push_str("# TYPE per_symbol_intents_approved counter\n");
        output.push_str(&format!("per_symbol_intents_approved{{symbol=\"{}\"}} {}\n", symbol, count));
    }

    for (symbol, count) in &snap.per_symbol_intents_rejected {
        output.push_str("# HELP per_symbol_intents_rejected Intents rejected per symbol\n");
        output.push_str("# TYPE per_symbol_intents_rejected counter\n");
        output.push_str(&format!("per_symbol_intents_rejected{{symbol=\"{}\"}} {}\n", symbol, count));
    }

    output.push_str(&format!("kill_switch_active {}\n", if state.kill_switch.is_active() { 1 } else { 0 }));

    Response::builder()
        .header("Content-Type", "text/plain; version=0.0.4")
        .body(output)
        .unwrap()
}

async fn status_handler(State(state): State<ApiState>) -> Json<StatusResponse> {
    Json(StatusResponse {
        kill_switch: KillSwitchResponse {
            active: state.kill_switch.is_active(),
            activated_at_ns: state.kill_switch.activated_at_ns(),
            activation_count: state.kill_switch.activation_count(),
            open_orders: state.kill_switch.get_open_orders(),
        },
        metrics: state.metrics.snapshot(),
    })
}

async fn shutdown_handler(State(state): State<ApiState>) -> StatusCode {
    send_command(&state, ControlCommand::Shutdown    )
}

// Circuit breaker handlers
async fn circuit_breaker_status(State(state): State<ApiState>) -> Json<Vec<CircuitBreakerStatusResponse>> {
    let states = state.execution_states.lock().unwrap();
    let mut result = Vec::new();
    for (symbol, exec_state) in states.iter() {
        let cb = &exec_state.circuit_breaker;
        result.push(CircuitBreakerStatusResponse {
            symbol: symbol.clone(),
            state: cb.state_name().to_string(),
            is_open: cb.is_open.load(std::sync::atomic::Ordering::Relaxed),
            failure_count: cb.failure_count(),
        });
    }
    Json(result)
}

async fn trip_circuit_breaker(State(state): State<ApiState>) -> StatusCode {
    send_command(&state, ControlCommand::CircuitBreakerTrip    )
}

async fn reset_circuit_breaker(State(state): State<ApiState>) -> StatusCode {
    send_command(&state, ControlCommand::CircuitBreakerReset    )
}

async fn set_circuit_breaker_config_handler(
    State(state): State<ApiState>,
    Json(req): Json<CircuitBreakerConfigRequest>,
) -> StatusCode {
    send_command(&state, ControlCommand::SetCircuitBreakerParams(CircuitBreakerConfigUpdate {
        failure_threshold: req.failure_threshold,
        cooldown_ms: req.cooldown_ms,
    })    )
}

// Asset control handlers
async fn pause_asset_handler(
    State(state): State<ApiState>,
    axum::extract::Path(symbol): axum::extract::Path<String>,
) -> StatusCode {
    send_command(&state, ControlCommand::PauseAsset(symbol)    )
}

async fn resume_asset_handler(
    State(state): State<ApiState>,
    axum::extract::Path(symbol): axum::extract::Path<String>,
) -> StatusCode {
    send_command(&state, ControlCommand::ResumeAsset(symbol)    )
}

async fn set_asset_config_handler(
    State(state): State<ApiState>,
    axum::extract::Path(symbol): axum::extract::Path<String>,
    Json(req): Json<AssetConfigRequest>,
) -> StatusCode {
    send_command(&state, ControlCommand::SetAssetConfig {
        symbol,
        config: AssetConfigUpdate {
            enabled: req.enabled,
            max_position: req.max_position,
            tick_size: req.tick_size,
        },
    }    )
}

async fn get_asset_config_handler(
    State(state): State<ApiState>,
    axum::extract::Path(symbol): axum::extract::Path<String>,
) -> Result<Json<AssetConfigResponse>, StatusCode> {
    let config = state.config.read();
    if let Some(asset) = config.asset_configs.iter().find(|a| a.symbol == symbol) {
        Ok(Json(AssetConfigResponse {
            symbol: asset.symbol.clone(),
            enabled: asset.enabled,
            max_position: asset.max_position,
            tick_size: asset.tick_size,
        }))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

// Mode handler
async fn set_mode_handler(
    State(state): State<ApiState>,
    Json(req): Json<ModeRequest>,
) -> StatusCode {
    send_command(&state, ControlCommand::SetMode(req.mode)    )
}

// Paper trading toggle handler (accepts bool per checklist)
async fn set_paper_trading_handler(
    State(state): State<ApiState>,
    Json(req): Json<PaperTradingRequest>,
) -> StatusCode {
    let mode = if req.paper_trading { "paper" } else { "live" };
    send_command(&state, ControlCommand::SetMode(mode.to_string())    )
}

// Risk parameters handlers
async fn set_risk_params_handler(
    State(state): State<ApiState>,
    Json(req): Json<RiskParamsRequest>,
) -> StatusCode {
    send_command(&state, ControlCommand::SetRiskParams(RiskConfigUpdate {
        max_portfolio_exposure: req.max_portfolio_exposure,
        max_leverage: req.max_leverage,
        max_drawdown_pct: req.max_drawdown_pct,
        max_order_rate_per_sec: req.max_order_rate_per_sec,
        max_position_per_symbol: req.max_position_per_symbol,
        max_volatility: req.max_volatility,
        max_spread_bps: req.max_spread_bps,
        max_slippage_bps: req.max_slippage_bps,
        allow_short: req.allow_short,
        kill_switch_on_drawdown: req.kill_switch_on_drawdown,
        risk_intent_staleness_ns: req.risk_intent_staleness_ns,
        initial_equity: req.initial_equity,
    })    )
}

async fn get_risk_params_handler(State(state): State<ApiState>) -> Json<RiskParamsResponse> {
    let config = state.config.read();
    let risk = &config.risk_config;
    Json(RiskParamsResponse {
        max_portfolio_exposure: risk.max_portfolio_exposure,
        max_leverage: risk.max_leverage,
        max_drawdown_pct: risk.max_drawdown_pct,
        max_order_rate_per_sec: risk.max_order_rate_per_sec,
        max_position_per_symbol: risk.max_position_per_symbol,
        max_volatility: risk.max_volatility,
        max_spread_bps: risk.max_spread_bps,
        max_slippage_bps: risk.max_slippage_bps,
        allow_short: risk.allow_short,
        kill_switch_on_drawdown: risk.kill_switch_on_drawdown,
        risk_intent_staleness_ns: risk.risk_intent_staleness_ns,
        initial_equity: risk.initial_equity,
    })
}

// Broker parameters handler
async fn set_broker_params_handler(
    State(state): State<ApiState>,
    Json(req): Json<BrokerParamsRequest>,
) -> StatusCode {
    send_command(&state, ControlCommand::SetBrokerParams(BrokerConfigUpdate {
        broker_type: req.broker_type,
        paper_trading: req.paper_trading,
        ws_url: req.ws_url,
        rest_url: req.rest_url,
        max_retries: req.max_retries,
        retry_backoff_ms: req.retry_backoff_ms,
    })    )
}

// Feature parameters handler
async fn set_feature_params_handler(
    State(state): State<ApiState>,
    Json(req): Json<FeatureParamsRequest>,
) -> StatusCode {
    send_command(&state, ControlCommand::SetFeatureParams(FeatureConfigUpdate {
        rsi_period: req.rsi_period,
        macd_fast: req.macd_fast,
        macd_slow: req.macd_slow,
        macd_signal: req.macd_signal,
        atr_period: req.atr_period,
        ema_periods: req.ema_periods,
        rolling_window_sizes: req.rolling_window_sizes,
        price_window_size: req.price_window_size,
        volume_window_size: req.volume_window_size,
        regime_volatile_atr_threshold: req.regime_volatile_atr_threshold,
        regime_trending_threshold: req.regime_trending_threshold,
    })    )
}

// Model parameters handler
async fn set_model_params_handler(
    State(state): State<ApiState>,
    Json(req): Json<ModelParamsRequest>,
) -> StatusCode {
    send_command(&state, ControlCommand::SetModelParams(ModelConfigUpdate {
        model_dir: req.model_dir,
        inference_threads: req.inference_threads,
        max_inference_latency_ms: req.max_inference_latency_ms,
        feature_vector_size: req.feature_vector_size,
        inference_rsi_bearish_threshold: req.inference_rsi_bearish_threshold,
        inference_rsi_bullish_threshold: req.inference_rsi_bullish_threshold,
        inference_rsi_center: req.inference_rsi_center,
        inference_atr_penalty_threshold: req.inference_atr_penalty_threshold,
        inference_volume_confirmation_threshold: req.inference_volume_confirmation_threshold,
        action_score_rsi_weight: req.action_score_rsi_weight,
        action_score_macd_weight: req.action_score_macd_weight,
        action_score_volatility_weight: req.action_score_volatility_weight,
        confidence_rsi_weight: req.confidence_rsi_weight,
        confidence_macd_weight: req.confidence_macd_weight,
        confidence_regime_weight: req.confidence_regime_weight,
    })    )
}

// Journal parameters handler
async fn set_journal_params_handler(
    State(state): State<ApiState>,
    Json(req): Json<JournalParamsRequest>,
) -> StatusCode {
    send_command(&state, ControlCommand::SetJournalParams(JournalConfigUpdate {
        journal_dir: req.journal_dir,
        flush_interval_ms: req.flush_interval_ms,
        snapshot_interval_sec: req.snapshot_interval_sec,
        max_file_size_mb: req.max_file_size_mb,
    })    )
}

async fn flush_journal_handler(State(state): State<ApiState>) -> StatusCode {
    send_command(&state, ControlCommand::FlushJournal    )
}

async fn reload_config_handler(State(state): State<ApiState>) -> StatusCode {
    send_command(&state, ControlCommand::ReloadConfig    )
}

// Execution defaults handler
async fn set_execution_defaults_handler(
    State(state): State<ApiState>,
    Json(req): Json<ExecutionDefaultsRequest>,
) -> StatusCode {
    send_command(&state, ControlCommand::SetExecutionDefaults(ExecutionDefaultsUpdate {
        default_order_quantity: req.default_order_quantity,
        execution_per_symbol_rate_divisor: req.execution_per_symbol_rate_divisor,
    })    )
}

// Rate limits handler
async fn set_rate_limits_handler(
    State(state): State<ApiState>,
    Json(req): Json<RateLimitRequest>,
) -> StatusCode {
    send_command(&state, ControlCommand::SetRateLimits(RateLimitConfigUpdate {
        global_rate: req.global_rate,
        per_symbol_rate: req.per_symbol_rate,
    })    )
}

// Channel config handler
async fn set_channel_params_handler(
    State(state): State<ApiState>,
    Json(req): Json<ChannelConfigRequest>,
) -> StatusCode {
    send_command(&state, ControlCommand::SetChannelParams(ChannelConfigUpdate {
        per_asset_tick_channel_capacity: req.per_asset_tick_channel_capacity,
        feature_channel_capacity: req.feature_channel_capacity,
        risk_channel_capacity: req.risk_channel_capacity,
        decision_channel_capacity: req.decision_channel_capacity,
        lifecycle_channel_capacity: req.lifecycle_channel_capacity,
        command_channel_capacity: req.command_channel_capacity,
        journal_channel_capacity: req.journal_channel_capacity,
    })    )
}

// Reactor config handler
async fn set_reactor_params_handler(
    State(state): State<ApiState>,
    Json(req): Json<ReactorConfigRequest>,
) -> StatusCode {
    send_command(&state, ControlCommand::SetReactorParams(ReactorConfigUpdate {
        max_batch_size: req.max_batch_size,
        control_batch_size: req.control_batch_size,
        sleep_on_empty_us: req.sleep_on_empty_us,
        backpressure_log_interval: req.backpressure_log_interval,
    })    )
}

// Validator config handler
async fn set_validator_params_handler(
    State(state): State<ApiState>,
    Json(req): Json<ValidatorConfigRequest>,
) -> StatusCode {
    send_command(&state, ControlCommand::SetValidatorParams(ValidatorConfigUpdate {
        max_symbol_length: req.max_symbol_length,
        max_quantity: req.max_quantity,
        max_order_id_length: req.max_order_id_length,
    })    )
}

// GET handlers for new config types
async fn get_channel_params_handler(State(state): State<ApiState>) -> Json<ChannelConfigResponse> {
    let config = state.config.read();
    let c = &config.channel_config;
    Json(ChannelConfigResponse {
        per_asset_tick_channel_capacity: c.per_asset_tick_channel_capacity,
        feature_channel_capacity: c.feature_channel_capacity,
        risk_channel_capacity: c.risk_channel_capacity,
        decision_channel_capacity: c.decision_channel_capacity,
        lifecycle_channel_capacity: c.lifecycle_channel_capacity,
        command_channel_capacity: c.command_channel_capacity,
        journal_channel_capacity: c.journal_channel_capacity,
    })
}

async fn get_reactor_params_handler(State(state): State<ApiState>) -> Json<ReactorConfigResponse> {
    let config = state.config.read();
    let c = &config.reactor_config;
    Json(ReactorConfigResponse {
        max_batch_size: c.max_batch_size,
        control_batch_size: c.control_batch_size,
        sleep_on_empty_us: c.sleep_on_empty_us,
        backpressure_log_interval: c.backpressure_log_interval,
    })
}

async fn get_validator_params_handler(State(state): State<ApiState>) -> Json<ValidatorConfigResponse> {
    let config = state.config.read();
    let c = &config.validator_config;
    Json(ValidatorConfigResponse {
        max_symbol_length: c.max_symbol_length,
        max_quantity: c.max_quantity,
        max_order_id_length: c.max_order_id_length,
    })
}

#[derive(Serialize)]
pub struct PerSymbolStatsResponse {
    pub per_symbol_ticks: std::collections::HashMap<String, u64>,
    pub per_symbol_features: std::collections::HashMap<String, u64>,
    pub per_symbol_intents_approved: std::collections::HashMap<String, u64>,
    pub per_symbol_intents_rejected: std::collections::HashMap<String, u64>,
}

async fn per_symbol_stats_handler(State(state): State<ApiState>) -> Json<PerSymbolStatsResponse> {
    let snap = state.metrics.snapshot();
    Json(PerSymbolStatsResponse {
        per_symbol_ticks: snap.per_symbol_ticks,
        per_symbol_features: snap.per_symbol_features,
        per_symbol_intents_approved: snap.per_symbol_intents_approved,
        per_symbol_intents_rejected: snap.per_symbol_intents_rejected,
    })
}

// Model handlers
async fn get_active_model_handler(State(state): State<ApiState>) -> Json<ActiveModelResponse> {
    match state.model_registry.get_active() {
        Some(info) => Json(ActiveModelResponse {
            model_id: info.model_id,
            version: info.version,
            input_features: info.input_features,
            applicable_regimes: info.applicable_regimes,
            priority: info.priority,
        }),
        None => Json(ActiveModelResponse {
            model_id: String::new(),
            version: 0,
            input_features: Vec::new(),
            applicable_regimes: Vec::new(),
            priority: 0,
        }),
    }
}

async fn list_models_handler(State(state): State<ApiState>) -> Json<ModelListResponse> {
    let models = state.model_registry.list_models();
    Json(ModelListResponse {
        models: models.into_iter().map(|info| ActiveModelResponse {
            model_id: info.model_id,
            version: info.version,
            input_features: info.input_features,
            applicable_regimes: info.applicable_regimes,
            priority: info.priority,
        }).collect(),
    })
}

async fn swap_model_handler(
    State(state): State<ApiState>,
    Json(req): Json<SwapModelRequest>,
) -> StatusCode {
    send_command(&state, ControlCommand::ModelSwap(req.model_id)    )
}

// Portfolio status handler
async fn portfolio_handler(State(state): State<ApiState>) -> Json<PortfolioResponse> {
    Json(PortfolioResponse {
        positions: state.position_manager.get_all_positions(),
        total_unrealized_pnl: state.position_manager.total_unrealized_pnl(),
        total_realized_pnl: state.position_manager.total_realized_pnl(),
        total_market_value: state.position_manager.total_market_value(),
        position_count: state.position_manager.position_count(),
    })
}

// Idempotency status handler
async fn idempotency_status_handler(State(state): State<ApiState>) -> Json<IdempotencyStatusResponse> {
    let states = state.execution_states.lock().unwrap();
    let total_size: usize = states.values().map(|s| s.idempotency_store.len()).sum();
    let total_capacity: usize = states.values().map(|s| s.idempotency_store.capacity()).sum();
    Json(IdempotencyStatusResponse {
        store_size: total_size,
        store_capacity: total_capacity,
    })
}

// Orders status handler
async fn orders_handler(State(state): State<ApiState>) -> Json<OrdersResponse> {
    let states = state.execution_states.lock().unwrap();
    let mut all_orders = Vec::new();
    for (_, exec_state) in states.iter() {
        let tracker = exec_state.order_tracker.lock().unwrap();
        for (_, order) in tracker.orders.iter() {
            all_orders.push(order.clone());
        }
    }
    Json(OrdersResponse { orders: all_orders })
}

// Tracked orders handler (from kill switch)
async fn tracked_orders_handler(State(state): State<ApiState>) -> Json<TrackedOrdersResponse> {
    Json(TrackedOrdersResponse {
        open_orders: state.kill_switch.get_open_orders(),
    })
}

// Rate limiter status handler
async fn rate_limiter_status_handler(State(state): State<ApiState>) -> Json<Vec<RateLimiterStatusResponse>> {
    let states = state.execution_states.lock().unwrap();
    let mut result = Vec::new();
    for (symbol, exec_state) in states.iter() {
        let rl = exec_state.rate_limiter.lock().unwrap();
        let global = rl.global_tokens_remaining();
        let (sym_tok, sym_pct, _) = rl.get_back_pressure_status(symbol, 0.0)
            .unwrap_or((0.0, 0.0, false));
        result.push(RateLimiterStatusResponse {
            symbol: symbol.clone(),
            global_tokens: global,
            symbol_tokens: sym_tok,
            symbol_percent_remaining: sym_pct,
        });
    }
    Json(result)
}

// Heartbeats handler
async fn heartbeats_handler(State(state): State<ApiState>) -> Json<HeartbeatsResponse> {
    let mut threads = Vec::new();
    if let Some(ref hb) = state.heartbeats {
        let map = hb.read();
        for (name, ts) in map.iter() {
            let last = ts.load(std::sync::atomic::Ordering::Relaxed);
            threads.push(HeartbeatDetail {
                thread: name.clone(),
                last_timestamp_ns: last,
            });
        }
    }
    Json(HeartbeatsResponse {
        status: "ok".to_string(),
        threads,
    })
}

// Kill switch orders handler
async fn kill_switch_orders_handler(State(state): State<ApiState>) -> Json<TrackedOrdersResponse> {
    Json(TrackedOrdersResponse {
        open_orders: state.kill_switch.get_open_orders(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::util::ServiceExt;

    fn test_state() -> ApiState {
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        let (tx, _rx) = crossbeam_channel::bounded::<ControlCommand>(100);
        let config = Arc::new(RwLock::new(EngineConfig::default()));
        let position_manager = Arc::new(PositionManager::new());
        ApiState {
            kill_switch: Arc::clone(&kill_switch),
            metrics: Arc::clone(&metrics),
            command_tx: tx,
            config,
            position_manager,
            heartbeats: None,
            execution_states: Arc::new(std::sync::Mutex::new(HashMap::new())),
            strategy_registry: Arc::new(std::sync::Mutex::new(HashMap::new())),
            model_registry: Arc::new(ModelRegistry::new()),
            api_key: String::new(),
            rate_limiter: SimpleRateLimiter::new(10, 1),
        }
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let state = test_state();
        let app = create_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_metrics_endpoint() {
        let state = test_state();
        state.metrics.ticks_processed.fetch_add(42, std::sync::atomic::Ordering::Relaxed);
        let app = create_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_prometheus_endpoint() {
        let state = test_state();
        state.metrics.ticks_processed.fetch_add(42, std::sync::atomic::Ordering::Relaxed);
        state.metrics.orders_filled.fetch_add(10, std::sync::atomic::Ordering::Relaxed);
        let app = create_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics/prometheus")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_kill_switch_status() {
        let state = test_state();
        let app = create_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health/kill-switch")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_set_kill_switch() {
        let (tx, rx) = crossbeam_channel::bounded::<ControlCommand>(100);
        let state = ApiState {
            kill_switch: Arc::new(KillSwitch::new()),
            metrics: Arc::new(GlobalMetrics::new()),
            command_tx: tx,
            config: Arc::new(RwLock::new(EngineConfig::default())),
            position_manager: Arc::new(PositionManager::new()),
            heartbeats: None,
            execution_states: Arc::new(std::sync::Mutex::new(HashMap::new())),
            strategy_registry: Arc::new(std::sync::Mutex::new(HashMap::new())),
            model_registry: Arc::new(ModelRegistry::new()),
            api_key: String::new(),
            rate_limiter: SimpleRateLimiter::new(10, 1),
        };
        let app = create_router(state);
        let body = serde_json::json!({ "active": true });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/control/kill-switch")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let cmd = rx.try_recv().unwrap();
        assert!(matches!(cmd, ControlCommand::SetKillSwitch(true)));
    }

    #[tokio::test]
    async fn test_status_endpoint() {
        let state = test_state();
        let app = create_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_shutdown_endpoint() {
        let (tx, rx) = crossbeam_channel::bounded::<ControlCommand>(100);
        let state = ApiState {
            kill_switch: Arc::new(KillSwitch::new()),
            metrics: Arc::new(GlobalMetrics::new()),
            command_tx: tx,
            config: Arc::new(RwLock::new(EngineConfig::default())),
            position_manager: Arc::new(PositionManager::new()),
            heartbeats: None,
            execution_states: Arc::new(std::sync::Mutex::new(HashMap::new())),
            strategy_registry: Arc::new(std::sync::Mutex::new(HashMap::new())),
            model_registry: Arc::new(ModelRegistry::new()),
            api_key: String::new(),
            rate_limiter: SimpleRateLimiter::new(10, 1),
        };
        let app = create_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/shutdown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let cmd = rx.try_recv().unwrap();
        assert!(matches!(cmd, ControlCommand::Shutdown));
    }

    #[tokio::test]
    async fn test_strategy_swap_endpoint() {
        let (tx, rx) = crossbeam_channel::bounded::<ControlCommand>(100);
        let state = ApiState {
            kill_switch: Arc::new(KillSwitch::new()),
            metrics: Arc::new(GlobalMetrics::new()),
            command_tx: tx,
            config: Arc::new(RwLock::new(EngineConfig::default())),
            position_manager: Arc::new(PositionManager::new()),
            heartbeats: None,
            execution_states: Arc::new(std::sync::Mutex::new(HashMap::new())),
            strategy_registry: Arc::new(std::sync::Mutex::new(HashMap::new())),
            model_registry: Arc::new(ModelRegistry::new()),
            api_key: String::new(),
            rate_limiter: SimpleRateLimiter::new(10, 1),
        };
        let app = create_router(state);
        let body = serde_json::json!({
            "symbol": "AAPL",
            "strategy_type": "aggressive",
            "params": null
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/control/strategy-swap")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let cmd = rx.try_recv().unwrap();
        match cmd {
            ControlCommand::SwapStrategy { symbol, strategy_type, params } => {
                assert_eq!(symbol, "AAPL");
                assert_eq!(strategy_type, "aggressive");
                assert!(params.is_none());
            }
            _ => panic!("Expected SwapStrategy command"),
        }
    }

    #[tokio::test]
    async fn test_strategy_swap_with_custom_params() {
        let (tx, rx) = crossbeam_channel::bounded::<ControlCommand>(100);
        let state = ApiState {
            kill_switch: Arc::new(KillSwitch::new()),
            metrics: Arc::new(GlobalMetrics::new()),
            command_tx: tx,
            config: Arc::new(RwLock::new(EngineConfig::default())),
            position_manager: Arc::new(PositionManager::new()),
            heartbeats: None,
            execution_states: Arc::new(std::sync::Mutex::new(HashMap::new())),
            strategy_registry: Arc::new(std::sync::Mutex::new(HashMap::new())),
            model_registry: Arc::new(ModelRegistry::new()),
            api_key: String::new(),
            rate_limiter: SimpleRateLimiter::new(10, 1),
        };
        let app = create_router(state);
        let body = serde_json::json!({
            "symbol": "AAPL",
            "strategy_type": "custom",
            "params": {
                "long_entry_threshold": 0.7,
                "short_entry_threshold": -0.7,
                "exit_threshold": 0.2,
                "confidence_minimum": 0.6,
                "hysteresis_deadband": 0.2,
                "entry_cooldown_ms": 3000,
                "exit_cooldown_ms": 1500,
                "prediction_staleness_ns": 100000000,
                "allow_short": false,
                "max_long_units": 100.0,
                "max_short_units": 100.0,
                "trade_intent_ttl_ns": 30000000000_i64,
                "urgency_aggressive_threshold": 0.85,
                "urgency_normal_threshold": 0.5
            }
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/control/strategy-swap")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let cmd = rx.try_recv().unwrap();
        match cmd {
            ControlCommand::SwapStrategy { symbol, strategy_type, params } => {
                assert_eq!(symbol, "AAPL");
                assert_eq!(strategy_type, "custom");
                assert!(params.is_some());
                let p = params.unwrap();
                assert_eq!(p.long_entry_threshold, 0.7);
                assert_eq!(p.allow_short, false);
            }
            _ => panic!("Expected SwapStrategy command"),
        }
    }

    #[tokio::test]
    async fn test_auth_rejects_without_key() {
        let (tx, _rx) = crossbeam_channel::bounded::<ControlCommand>(100);
        let state = ApiState {
            kill_switch: Arc::new(KillSwitch::new()),
            metrics: Arc::new(GlobalMetrics::new()),
            command_tx: tx,
            config: Arc::new(RwLock::new(EngineConfig::default())),
            position_manager: Arc::new(PositionManager::new()),
            heartbeats: None,
            execution_states: Arc::new(std::sync::Mutex::new(HashMap::new())),
            strategy_registry: Arc::new(std::sync::Mutex::new(HashMap::new())),
            model_registry: Arc::new(ModelRegistry::new()),
            api_key: "secret-key".to_string(),
            rate_limiter: SimpleRateLimiter::new(10, 1),
        };
        let app = create_router(state);
        let body = serde_json::json!({ "active": true });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/control/kill-switch")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_auth_allows_with_valid_key() {
        let (tx, rx) = crossbeam_channel::bounded::<ControlCommand>(100);
        let state = ApiState {
            kill_switch: Arc::new(KillSwitch::new()),
            metrics: Arc::new(GlobalMetrics::new()),
            command_tx: tx,
            config: Arc::new(RwLock::new(EngineConfig::default())),
            position_manager: Arc::new(PositionManager::new()),
            heartbeats: None,
            execution_states: Arc::new(std::sync::Mutex::new(HashMap::new())),
            strategy_registry: Arc::new(std::sync::Mutex::new(HashMap::new())),
            model_registry: Arc::new(ModelRegistry::new()),
            api_key: "secret-key".to_string(),
            rate_limiter: SimpleRateLimiter::new(10, 1),
        };
        let app = create_router(state);
        let body = serde_json::json!({ "active": true });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/control/kill-switch")
                    .header("content-type", "application/json")
                    .header("Authorization", "Bearer secret-key")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let cmd = rx.try_recv().unwrap();
        assert!(matches!(cmd, ControlCommand::SetKillSwitch(true)));
    }

    #[tokio::test]
    async fn test_rate_limit_throttles_requests() {
        let (tx, _rx) = crossbeam_channel::bounded::<ControlCommand>(100);
        let state = ApiState {
            kill_switch: Arc::new(KillSwitch::new()),
            metrics: Arc::new(GlobalMetrics::new()),
            command_tx: tx,
            config: Arc::new(RwLock::new(EngineConfig::default())),
            position_manager: Arc::new(PositionManager::new()),
            heartbeats: None,
            execution_states: Arc::new(std::sync::Mutex::new(HashMap::new())),
            strategy_registry: Arc::new(std::sync::Mutex::new(HashMap::new())),
            model_registry: Arc::new(ModelRegistry::new()),
            api_key: String::new(),
            rate_limiter: SimpleRateLimiter::new(1, 60),
        };
        let app = create_router(state);
        let body = serde_json::json!({ "active": true });

        // First request should pass
        let response1 = app.clone().oneshot(
            Request::builder()
                .method("POST")
                .uri("/control/kill-switch")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .unwrap(),
        ).await.unwrap();
        assert_eq!(response1.status(), StatusCode::OK);

        // Second request should be rate limited
        let response2 = app.oneshot(
            Request::builder()
                .method("POST")
                .uri("/control/kill-switch")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .unwrap(),
        ).await.unwrap();
        assert_eq!(response2.status(), StatusCode::TOO_MANY_REQUESTS);
    }
}
