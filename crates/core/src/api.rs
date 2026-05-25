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
use crate::command_channel::ControlCommand;

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
pub struct StrategySwapRequest {
    pub symbol: String,
    pub strategy_type: String,
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

pub fn create_router(state: ApiState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/health/kill-switch", get(kill_switch_status))
        .route("/control/kill-switch", post(set_kill_switch))
        .route("/control/strategy-swap", post(strategy_swap_handler))
        .route("/metrics", get(metrics_handler))
        .route("/metrics/prometheus", get(prometheus_handler))
        .route("/status", get(status_handler))
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
    let cmd = ControlCommand::SwapStrategy {
        symbol: req.symbol.clone(),
        strategy_type: req.strategy_type.clone(),
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
            "strategy_type": "aggressive"
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
            ControlCommand::SwapStrategy { symbol, strategy_type } => {
                assert_eq!(symbol, "AAPL");
                assert_eq!(strategy_type, "aggressive");
            }
            _ => panic!("Expected SwapStrategy command"),
        }
    }
}
