use axum::{
    extract::State,
    http::StatusCode,
    response::{Json, Response},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::kill_switch::KillSwitch;
use crate::metrics::{GlobalMetrics, MetricsSnapshot};
use crate::command_channel::{ControlCommand, StrategyParams};

#[derive(Clone)]
pub struct ApiState {
    pub kill_switch: Arc<KillSwitch>,
    pub metrics: Arc<GlobalMetrics>,
    pub command_tx: crossbeam_channel::Sender<ControlCommand>,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub kill_switch_active: bool,
    pub uptime: String,
}

#[derive(Serialize)]
pub struct MetricsResponse {
    pub metrics: MetricsSnapshot,
}

#[derive(Serialize)]
pub struct KillSwitchResponse {
    pub active: bool,
    pub activated_at_ns: u64,
    pub activation_count: u64,
}

#[derive(Deserialize)]
pub struct KillSwitchRequest {
    pub active: bool,
}

#[derive(Deserialize)]
pub struct StrategyParamsRequest {
    pub long_entry_threshold: f64,
    pub short_entry_threshold: f64,
    pub confidence_minimum: f64,
    pub hysteresis_deadband: f64,
    pub entry_cooldown_ms: u64,
    pub exit_cooldown_ms: u64,
    pub prediction_staleness_ns: u64,
    pub allow_short: bool,
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
    pub failure_count: u32,
    pub success_count: u32,
    pub last_failure_ns: u64,
}

#[derive(Deserialize)]
pub struct CircuitBreakerActionRequest {
    pub action: String, // "trip" or "reset"
}

#[derive(Deserialize)]
pub struct AssetControlRequest {
    pub symbol: String,
}

#[derive(Deserialize)]
pub struct ModeRequest {
    pub mode: String,
}

#[derive(Deserialize)]
pub struct RiskParamsRequest {
    pub max_position_per_symbol: f64,
    pub max_portfolio_exposure: f64,
    pub max_leverage: f64,
    pub max_order_rate_per_sec: u32,
}

#[derive(Serialize)]
pub struct HeartbeatsResponse {
    pub status: String,
    pub threads: Vec<String>,
}

pub fn create_router(state: ApiState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/health/kill-switch", get(kill_switch_status))
        .route("/health/heartbeats", get(heartbeats_handler))
        .route("/control/kill-switch", post(set_kill_switch))
        .route("/control/circuit-breaker", get(circuit_breaker_status))
        .route("/control/circuit-breaker/trip", post(trip_circuit_breaker))
        .route("/control/circuit-breaker/reset", post(reset_circuit_breaker))
        .route("/control/strategy-swap", post(strategy_swap_handler))
        .route("/control/asset/:symbol/pause", post(pause_asset_handler))
        .route("/control/asset/:symbol/resume", post(resume_asset_handler))
        .route("/control/mode", post(set_mode_handler))
        .route("/control/risk/parameters", post(set_risk_params_handler))
        .route("/metrics", get(metrics_handler))
        .route("/metrics/prometheus", get(prometheus_handler))
        .route("/status", get(status_handler))
        .route("/status/circuit-breaker", get(circuit_breaker_status))
        .route("/status/risk/portfolio", get(risk_portfolio_handler))
        .route("/shutdown", post(shutdown_handler))
        .with_state(state)
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
    match state.command_tx.send(cmd) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

async fn strategy_swap_handler(
    State(state): State<ApiState>,
    Json(req): Json<StrategySwapRequest>,
) -> (StatusCode, Json<StrategySwapResponse>) {
    let params = req.params.map(|p| StrategyParams {
        long_entry_threshold: p.long_entry_threshold,
        short_entry_threshold: p.short_entry_threshold,
        confidence_minimum: p.confidence_minimum,
        hysteresis_deadband: p.hysteresis_deadband,
        entry_cooldown_ms: p.entry_cooldown_ms,
        exit_cooldown_ms: p.exit_cooldown_ms,
        prediction_staleness_ns: p.prediction_staleness_ns,
        allow_short: p.allow_short,
    });
    
    let cmd = ControlCommand::SwapStrategy {
        symbol: req.symbol.clone(),
        strategy_type: req.strategy_type.clone(),
        params,
    };
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
        },
        metrics: state.metrics.snapshot(),
    })
}

async fn shutdown_handler(State(state): State<ApiState>) -> StatusCode {
    match state.command_tx.send(ControlCommand::Shutdown) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

// Circuit breaker handlers
async fn circuit_breaker_status(State(state): State<ApiState>) -> Json<CircuitBreakerResponse> {
    // Note: Circuit breaker state would need to be passed through ApiState
    // For now, return placeholder
    Json(CircuitBreakerResponse {
        is_open: false,
        failure_count: 0,
        success_count: 0,
        last_failure_ns: 0,
    })
}

async fn trip_circuit_breaker(State(state): State<ApiState>) -> StatusCode {
    match state.command_tx.send(ControlCommand::CircuitBreakerTrip) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

async fn reset_circuit_breaker(State(state): State<ApiState>) -> StatusCode {
    match state.command_tx.send(ControlCommand::CircuitBreakerReset) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

// Asset control handlers
async fn pause_asset_handler(
    State(state): State<ApiState>,
    axum::extract::Path(symbol): axum::extract::Path<String>,
) -> StatusCode {
    match state.command_tx.send(ControlCommand::PauseAsset(symbol)) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

async fn resume_asset_handler(
    State(state): State<ApiState>,
    axum::extract::Path(symbol): axum::extract::Path<String>,
) -> StatusCode {
    match state.command_tx.send(ControlCommand::ResumeAsset(symbol)) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

// Mode handler
async fn set_mode_handler(
    State(state): State<ApiState>,
    Json(req): Json<ModeRequest>,
) -> StatusCode {
    match state.command_tx.send(ControlCommand::SetMode(req.mode)) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

// Risk parameters handler
async fn set_risk_params_handler(
    State(state): State<ApiState>,
    Json(req): Json<RiskParamsRequest>,
) -> StatusCode {
    match state.command_tx.send(ControlCommand::SetRiskParams(crate::command_channel::RiskConfigUpdate {
        max_position_per_symbol: req.max_position_per_symbol,
        max_portfolio_exposure: req.max_portfolio_exposure,
        max_leverage: req.max_leverage,
        max_order_rate_per_sec: req.max_order_rate_per_sec,
    })) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

// Risk portfolio status handler
async fn risk_portfolio_handler(State(state): State<ApiState>) -> Json<serde_json::Value> {
    let snap = state.metrics.snapshot();
    Json(serde_json::json!({
        "status": "ok",
        "metrics": snap,
    }))
}

// Heartbeats handler
async fn heartbeats_handler(State(state): State<ApiState>) -> Json<HeartbeatsResponse> {
    Json(HeartbeatsResponse {
        status: "ok".to_string(),
        threads: vec![
            "alpaca-feed".to_string(),
            "lifecycle-handler".to_string(),
            "prediction".to_string(),
            "risk-coordinator".to_string(),
            "journal".to_string(),
        ],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn test_health_endpoint() {
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        let (tx, _rx) = crossbeam_channel::bounded::<ControlCommand>(100);

        let state = ApiState {
            kill_switch: Arc::clone(&kill_switch),
            metrics: Arc::clone(&metrics),
            command_tx: tx,
        };

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
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        metrics.ticks_processed.fetch_add(42, std::sync::atomic::Ordering::Relaxed);
        let (tx, _rx) = crossbeam_channel::bounded::<ControlCommand>(100);

        let state = ApiState {
            kill_switch: Arc::clone(&kill_switch),
            metrics: Arc::clone(&metrics),
            command_tx: tx,
        };

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
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        metrics.ticks_processed.fetch_add(42, std::sync::atomic::Ordering::Relaxed);
        metrics.orders_filled.fetch_add(10, std::sync::atomic::Ordering::Relaxed);
        let (tx, _rx) = crossbeam_channel::bounded::<ControlCommand>(100);

        let state = ApiState {
            kill_switch: Arc::clone(&kill_switch),
            metrics: Arc::clone(&metrics),
            command_tx: tx,
        };

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
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        let (tx, _rx) = crossbeam_channel::bounded::<ControlCommand>(100);

        let state = ApiState {
            kill_switch: Arc::clone(&kill_switch),
            metrics: Arc::clone(&metrics),
            command_tx: tx,
        };

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
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        let (tx, rx) = crossbeam_channel::bounded::<ControlCommand>(100);

        let state = ApiState {
            kill_switch: Arc::clone(&kill_switch),
            metrics: Arc::clone(&metrics),
            command_tx: tx,
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
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        let (tx, _rx) = crossbeam_channel::bounded::<ControlCommand>(100);

        let state = ApiState {
            kill_switch: Arc::clone(&kill_switch),
            metrics: Arc::clone(&metrics),
            command_tx: tx,
        };

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
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        let (tx, rx) = crossbeam_channel::bounded::<ControlCommand>(100);

        let state = ApiState {
            kill_switch: Arc::clone(&kill_switch),
            metrics: Arc::clone(&metrics),
            command_tx: tx,
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
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        let (tx, rx) = crossbeam_channel::bounded::<ControlCommand>(100);

        let state = ApiState {
            kill_switch: Arc::clone(&kill_switch),
            metrics: Arc::clone(&metrics),
            command_tx: tx,
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
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        let (tx, rx) = crossbeam_channel::bounded::<ControlCommand>(100);

        let state = ApiState {
            kill_switch: Arc::clone(&kill_switch),
            metrics: Arc::clone(&metrics),
            command_tx: tx,
        };

        let app = create_router(state);
        let body = serde_json::json!({
            "symbol": "AAPL",
            "strategy_type": "custom",
            "params": {
                "long_entry_threshold": 0.7,
                "short_entry_threshold": -0.7,
                "confidence_minimum": 0.6,
                "hysteresis_deadband": 0.2,
                "entry_cooldown_ms": 3000,
                "exit_cooldown_ms": 1500,
                "prediction_staleness_ns": 100000000,
                "allow_short": false
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
}
