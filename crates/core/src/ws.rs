use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use futures_util::SinkExt;
use serde::Serialize;
use std::sync::Arc;
use tokio::time::{interval, Duration};

use crate::metrics::GlobalMetrics;
use crate::kill_switch::KillSwitch;

#[derive(Clone)]
pub struct WsState {
    pub metrics: Arc<GlobalMetrics>,
    pub kill_switch: Arc<KillSwitch>,
}

#[derive(Serialize)]
struct TelemetryEvent {
    pub event_type: String,
    pub data: serde_json::Value,
}

pub fn create_ws_router(state: WsState) -> Router {
    Router::new()
        .route("/ws/telemetry", get(ws_handler))
        .with_state(state)
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<WsState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: WsState) {
    let mut write = socket;
    let mut ticker = interval(Duration::from_millis(500));

    loop {
        ticker.tick().await;

        let metrics = state.metrics.snapshot();
        let event = TelemetryEvent {
            event_type: "metrics".to_string(),
            data: serde_json::to_value(&metrics).unwrap_or_default(),
        };

        let msg = serde_json::to_string(&event).unwrap_or_default();
        if write.send(Message::Text(msg.into())).await.is_err() {
            break;
        }

        if state.kill_switch.is_active() {
            let event = TelemetryEvent {
                event_type: "kill_switch".to_string(),
                data: serde_json::json!({
                    "active": true,
                    "activated_at_ns": state.kill_switch.activated_at_ns(),
                }),
            };
            let msg = serde_json::to_string(&event).unwrap_or_default();
            let _ = write.send(Message::Text(msg.into())).await;
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_telemetry_event_serialization() {
        let metrics = GlobalMetrics::new();
        let snap = metrics.snapshot();
        let event = TelemetryEvent {
            event_type: "metrics".to_string(),
            data: serde_json::to_value(&snap).unwrap(),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("metrics"));
        assert!(json.contains("ticks_processed"));
    }
}
