use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rand::Rng;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct OrderCommand {
    pub order_id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub quantity: f64,
    pub order_type: OrderType,
    pub limit_price: Option<f64>,
    pub stop_price: Option<f64>,
    pub time_in_force: TimeInForce,
    pub correlation_id: String,
    /// Trace ID propagated from RawTick for causal tracing.
    pub trace_id: u64,
}

#[derive(Debug, Clone)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone)]
pub enum OrderType {
    Market,
    Limit,
    Stop,
    StopLimit,
}

#[derive(Debug, Clone)]
pub enum TimeInForce {
    Day,
    Gtc,
    Ioc,
    Fok,
}

#[derive(Debug, Clone)]
pub struct CancelCommand {
    pub execution_id: String,
}

#[derive(Debug, Clone)]
pub struct ReplaceCommand {
    pub execution_id: String,
    pub symbol: String,
    pub quantity: Option<f64>,
    pub limit_price: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct StatusQuery {
    pub execution_id: String,
}

#[derive(Debug, Clone)]
pub struct OrderStatusResponse {
    pub execution_id: String,
    pub status: String,
    pub symbol: String,
    pub side: OrderSide,
    pub total_qty: u32,
    pub filled_qty: u32,
    pub remaining_qty: u32,
    pub avg_fill_price: Option<f64>,
}

impl OrderStatusResponse {
    pub fn new(execution_id: String, status: String, symbol: String, side: OrderSide, total_qty: u32) -> Self {
        Self {
            execution_id,
            status,
            symbol,
            side,
            total_qty,
            filled_qty: 0,
            remaining_qty: total_qty,
            avg_fill_price: None,
        }
    }

    pub fn with_fill(mut self, filled_qty: u32, avg_fill_price: f64) -> Self {
        self.filled_qty = filled_qty;
        self.avg_fill_price = Some(avg_fill_price);
        self.remaining_qty = self.total_qty.saturating_sub(filled_qty);
        self
    }
}

#[derive(Debug, Clone)]
pub struct OpenOrderInfo {
    pub order_id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub quantity: f64,
    pub filled_qty: f64,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct PositionInfo {
    pub symbol: String,
    pub qty: f64,
    pub avg_entry_price: f64,
    pub current_price: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BrokerError {
    ConnectionFailed(String),
    Rejected(String),
    ParseFailed(String),
    AuthFailed,
    RateLimited,
    Unknown(String),
}

impl std::fmt::Display for BrokerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BrokerError::ConnectionFailed(msg) => write!(f, "Connection failed: {}", msg),
            BrokerError::Rejected(msg) => write!(f, "Rejected: {}", msg),
            BrokerError::ParseFailed(msg) => write!(f, "Parse failed: {}", msg),
            BrokerError::AuthFailed => write!(f, "Authentication failed"),
            BrokerError::RateLimited => write!(f, "Rate limited"),
            BrokerError::Unknown(msg) => write!(f, "Unknown error: {}", msg),
        }
    }
}

impl std::error::Error for BrokerError {}

pub trait IExecutionPort: Send + Sync {
    fn submit_order(&self, cmd: &OrderCommand) -> Result<String, BrokerError>;
    fn cancel_order(&self, cmd: &CancelCommand) -> Result<(), BrokerError>;
    fn replace_order(&self, cmd: &ReplaceCommand) -> Result<String, BrokerError>;
    fn get_order_status(&self, query: &StatusQuery) -> Result<OrderStatusResponse, BrokerError>;
    fn query_open_orders(&self) -> Result<Vec<OpenOrderInfo>, BrokerError>;
    fn query_positions(&self) -> Result<Vec<PositionInfo>, BrokerError>;
}

/// Configuration for MockExecutionPort behavior.
#[derive(Debug, Clone)]
pub struct MockConfig {
    /// Probability (0.0 to 1.0) that an order submission will fail.
    pub failure_rate: f64,
    /// Simulated network latency in milliseconds.
    pub latency_ms: u64,
    /// Probability (0.0 to 1.0) that a fill will be partial instead of full.
    pub partial_fill_rate: f64,
    /// Fixed quantity for partial fills.
    pub partial_fill_qty: u32,
    /// If true, always fail regardless of failure_rate.
    pub explicit_failure: bool,
    /// Error message returned on failure.
    pub failure_message: String,
}

impl Default for MockConfig {
    fn default() -> Self {
        Self {
            failure_rate: 0.0,
            latency_ms: 0,
            partial_fill_rate: 0.0,
            partial_fill_qty: 0,
            explicit_failure: false,
            failure_message: "Mock execution failed".to_string(),
        }
    }
}

/// Builder for constructing MockExecutionPort with custom configuration.
#[derive(Debug)]
pub struct MockExecutionPortBuilder {
    config: MockConfig,
}

impl MockExecutionPortBuilder {
    pub fn new() -> Self {
        Self {
            config: MockConfig::default(),
        }
    }

    pub fn failure_rate(mut self, rate: f64) -> Self {
        self.config.failure_rate = rate.clamp(0.0, 1.0);
        self
    }

    pub fn latency_ms(mut self, ms: u64) -> Self {
        self.config.latency_ms = ms;
        self
    }

    pub fn partial_fill_rate(mut self, rate: f64) -> Self {
        self.config.partial_fill_rate = rate.clamp(0.0, 1.0);
        self
    }

    pub fn partial_fill_qty(mut self, qty: u32) -> Self {
        self.config.partial_fill_qty = qty;
        self
    }

    pub fn explicit_failure(mut self, enabled: bool) -> Self {
        self.config.explicit_failure = enabled;
        self
    }

    pub fn failure_message(mut self, msg: impl Into<String>) -> Self {
        self.config.failure_message = msg.into();
        self
    }

    pub fn build(self) -> MockExecutionPort {
        MockExecutionPort::with_config(self.config)
    }
}

impl Default for MockExecutionPortBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct AlpacaOrderRequest {
    pub(crate) symbol: String,
    pub(crate) qty: f64,
    pub(crate) side: String,
    pub(crate) r#type: String,
    pub(crate) time_in_force: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) limit_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) stop_price: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AlpacaOrderResponse {
    pub(crate) id: String,
    pub(crate) symbol: String,
    pub(crate) side: String,
    pub(crate) qty: String,
    pub(crate) filled_qty: String,
    pub(crate) r#type: String,
    pub(crate) status: String,
    #[serde(default)]
    pub(crate) filled_avg_price: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AlpacaCancelResponse {
    pub(crate) id: String,
    pub(crate) status: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AlpacaPosition {
    pub(crate) symbol: String,
    pub(crate) qty: String,
    pub(crate) avg_entry_price: String,
    pub(crate) current_price: String,
}

pub struct AlpacaExecutionPort {
    client: reqwest::blocking::Client,
    base_url: String,
    api_key: String,
    api_secret: String,
}

impl AlpacaExecutionPort {
    pub fn new(api_key: &str, api_secret: &str, paper: bool) -> Result<Self, BrokerError> {
        let base_url = if paper {
            "https://paper-api.alpaca.markets/v2"
        } else {
            "https://api.alpaca.markets/v2"
        };

        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| BrokerError::ConnectionFailed(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self {
            client,
            base_url: base_url.to_string(),
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
        })
    }

    fn headers(&self) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        headers.insert("APCA-API-KEY-ID".to_string(), self.api_key.clone());
        headers.insert("APCA-API-SECRET-KEY".to_string(), self.api_secret.clone());
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        headers
    }

    fn parse_side(side: &str) -> OrderSide {
        match side.to_lowercase().as_str() {
            "buy" => OrderSide::Buy,
            _ => OrderSide::Sell,
        }
    }
}

impl AlpacaExecutionPort {
    fn get_headers_map(&self) -> reqwest::header::HeaderMap {
        let mut h = reqwest::header::HeaderMap::new();
        h.insert("APCA-API-KEY-ID", reqwest::header::HeaderValue::from_str(&self.api_key).unwrap());
        h.insert("APCA-API-SECRET-KEY", reqwest::header::HeaderValue::from_str(&self.api_secret).unwrap());
        h
    }
}

impl IExecutionPort for AlpacaExecutionPort {
    fn submit_order(&self, cmd: &OrderCommand) -> Result<String, BrokerError> {
        let side = match cmd.side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        };

        let order_type = match cmd.order_type {
            OrderType::Market => "market",
            OrderType::Limit => "limit",
            OrderType::Stop => "stop",
            OrderType::StopLimit => "stop_limit",
        };

        let tif = match cmd.time_in_force {
            TimeInForce::Day => "day",
            TimeInForce::Gtc => "gtc",
            TimeInForce::Ioc => "ioc",
            TimeInForce::Fok => "fok",
        };

        let req = AlpacaOrderRequest {
            symbol: cmd.symbol.clone(),
            qty: cmd.quantity,
            side: side.to_string(),
            r#type: order_type.to_string(),
            time_in_force: tif.to_string(),
            limit_price: cmd.limit_price,
            stop_price: cmd.stop_price,
        };

        let url = format!("{}/orders", self.base_url);

        let response = self.client.post(&url)
            .headers(self.get_headers_map())
            .json(&req)
            .send()
            .map_err(|e| BrokerError::ConnectionFailed(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(BrokerError::Rejected(format!("Order rejected: {} - {}", status, body)));
        }

        let order: AlpacaOrderResponse = response.json()
            .map_err(|e| BrokerError::ParseFailed(format!("Failed to parse response: {}", e)))?;

        tracing::info!(order_id = %order.id, symbol = %order.symbol, "Order submitted to Alpaca");

        Ok(order.id)
    }

    fn cancel_order(&self, cmd: &CancelCommand) -> Result<(), BrokerError> {
        let url = format!("{}/orders/{}", self.base_url, cmd.execution_id);

        let response = self.client.delete(&url)
            .headers(self.get_headers_map())
            .send()
            .map_err(|e| BrokerError::ConnectionFailed(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(BrokerError::Rejected(format!("Cancel failed: {} - {}", status, body)));
        }

        tracing::info!(order_id = %cmd.execution_id, "Order cancelled on Alpaca");

        Ok(())
    }

    fn replace_order(&self, cmd: &ReplaceCommand) -> Result<String, BrokerError> {
        let url = format!("{}/orders/{}", self.base_url, cmd.execution_id);

        let mut body = serde_json::Map::new();
        if let Some(qty) = cmd.quantity {
            body.insert("qty".to_string(), serde_json::Value::Number(serde_json::Number::from_f64(qty).unwrap()));
        }
        if let Some(price) = cmd.limit_price {
            body.insert("limit_price".to_string(), serde_json::Value::Number(serde_json::Number::from_f64(price).unwrap()));
        }

        let response = self.client.patch(&url)
            .headers(self.get_headers_map())
            .json(&body)
            .send()
            .map_err(|e| BrokerError::ConnectionFailed(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(BrokerError::Rejected(format!("Replace failed: {} - {}", status, body)));
        }

        let order: AlpacaOrderResponse = response.json()
            .map_err(|e| BrokerError::ParseFailed(format!("Failed to parse response: {}", e)))?;

        tracing::info!(order_id = %order.id, "Order replaced on Alpaca");

        Ok(order.id)
    }

    fn get_order_status(&self, query: &StatusQuery) -> Result<OrderStatusResponse, BrokerError> {
        let url = format!("{}/orders/{}", self.base_url, query.execution_id);

        let response = self.client.get(&url)
            .headers(self.get_headers_map())
            .send()
            .map_err(|e| BrokerError::ConnectionFailed(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(BrokerError::Rejected(format!("Query failed: {} - {}", status, body)));
        }

        let order: AlpacaOrderResponse = response.json()
            .map_err(|e| BrokerError::ParseFailed(format!("Failed to parse response: {}", e)))?;

        let total_qty: u32 = order.qty.parse().unwrap_or(0);
        let filled_qty: u32 = order.filled_qty.parse().unwrap_or(0);
        let remaining_qty = total_qty.saturating_sub(filled_qty);
        let avg_fill_price = order.filled_avg_price.and_then(|p| p.parse().ok());

        let response = OrderStatusResponse::new(
            query.execution_id.clone(),
            order.status.clone(),
            order.symbol.clone(),
            Self::parse_side(&order.side),
            total_qty,
        )
        .with_fill(filled_qty, avg_fill_price.unwrap_or(0.0));

        tracing::info!(
            "Order status id={} status={:?} filled={}/{} avg_price={:?}",
            query.execution_id,
            order.status,
            filled_qty,
            total_qty,
            avg_fill_price
        );

        Ok(response)
    }

    fn query_open_orders(&self) -> Result<Vec<OpenOrderInfo>, BrokerError> {
        let url = format!("{}/orders?status=open", self.base_url);

        let response = self.client.get(&url)
            .headers(self.get_headers_map())
            .send()
            .map_err(|e| BrokerError::ConnectionFailed(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(BrokerError::Rejected(format!("Query open orders failed: {} - {}", status, body)));
        }

        let orders: Vec<AlpacaOrderResponse> = response.json()
            .map_err(|e| BrokerError::ParseFailed(format!("Failed to parse response: {}", e)))?;

        let mut result = Vec::new();
        for order in orders {
            let total_qty: f64 = order.qty.parse().unwrap_or(0.0);
            let filled_qty: f64 = order.filled_qty.parse().unwrap_or(0.0);
            result.push(OpenOrderInfo {
                order_id: order.id,
                symbol: order.symbol,
                side: Self::parse_side(&order.side),
                quantity: total_qty,
                filled_qty,
                status: order.status,
            });
        }

        tracing::info!(count = %result.len(), "Queried open orders from Alpaca");
        Ok(result)
    }

    fn query_positions(&self) -> Result<Vec<PositionInfo>, BrokerError> {
        let url = format!("{}/positions", self.base_url);

        let response = self.client.get(&url)
            .headers(self.get_headers_map())
            .send()
            .map_err(|e| BrokerError::ConnectionFailed(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(BrokerError::Rejected(format!("Query positions failed: {} - {}", status, body)));
        }

        let positions: Vec<AlpacaPosition> = response.json()
            .map_err(|e| BrokerError::ParseFailed(format!("Failed to parse response: {}", e)))?;

        let mut result = Vec::new();
        for pos in positions {
            let qty: f64 = pos.qty.parse().unwrap_or(0.0);
            let avg_entry: f64 = pos.avg_entry_price.parse().unwrap_or(0.0);
            let current: f64 = pos.current_price.parse().unwrap_or(0.0);
            result.push(PositionInfo {
                symbol: pos.symbol,
                qty,
                avg_entry_price: avg_entry,
                current_price: current,
            });
        }

        tracing::info!(count = %result.len(), "Queried positions from Alpaca");
        Ok(result)
    }
}

pub struct MockExecutionPort {
    pub open_orders: Vec<OpenOrderInfo>,
    pub positions: Vec<PositionInfo>,
    config: parking_lot::Mutex<MockConfig>,
    submit_count: AtomicU64,
    failure_count: AtomicU64,
}

impl MockExecutionPort {
    pub fn builder() -> MockExecutionPortBuilder {
        MockExecutionPortBuilder::new()
    }

    pub fn with_config(config: MockConfig) -> Self {
        Self {
            open_orders: Vec::new(),
            positions: Vec::new(),
            config: parking_lot::Mutex::new(config),
            submit_count: AtomicU64::new(0),
            failure_count: AtomicU64::new(0),
        }
    }

    pub fn config(&self) -> MockConfig {
        self.config.lock().clone()
    }

    pub fn update_config(&self, config: MockConfig) {
        *self.config.lock() = config;
    }

    pub fn submit_count(&self) -> u64 {
        self.submit_count.load(Ordering::Relaxed)
    }

    pub fn failure_count(&self) -> u64 {
        self.failure_count.load(Ordering::Relaxed)
    }

    fn should_fail(&self) -> bool {
        let config = self.config.lock();
        if config.explicit_failure {
            return true;
        }
        if config.failure_rate > 0.0 {
            let mut rng = rand::thread_rng();
            return rng.gen::<f64>() < config.failure_rate;
        }
        false
    }

    fn apply_latency(&self) {
        let latency_ms = {
            let config = self.config.lock();
            config.latency_ms
        };
        if latency_ms > 0 {
            std::thread::sleep(Duration::from_millis(latency_ms));
        }
    }

    fn should_partial_fill(&self) -> bool {
        let config = self.config.lock();
        if config.partial_fill_rate > 0.0 {
            let mut rng = rand::thread_rng();
            return rng.gen::<f64>() < config.partial_fill_rate;
        }
        false
    }
}

impl Default for MockExecutionPort {
    fn default() -> Self {
        Self::with_config(MockConfig::default())
    }
}

impl IExecutionPort for MockExecutionPort {
    fn submit_order(&self, cmd: &OrderCommand) -> Result<String, BrokerError> {
        self.submit_count.fetch_add(1, Ordering::Relaxed);
        self.apply_latency();

        if self.should_fail() {
            self.failure_count.fetch_add(1, Ordering::Relaxed);
            let config = self.config.lock();
            return Err(BrokerError::Rejected(config.failure_message.clone()));
        }

        Ok(format!("mock-{}", cmd.order_id))
    }

    fn cancel_order(&self, _cmd: &CancelCommand) -> Result<(), BrokerError> {
        Ok(())
    }

    fn replace_order(&self, cmd: &ReplaceCommand) -> Result<String, BrokerError> {
        Ok(format!("mock-replaced-{}", cmd.execution_id))
    }

    fn get_order_status(&self, query: &StatusQuery) -> Result<OrderStatusResponse, BrokerError> {
        let total_qty = 100u32;
        let filled_qty = if self.should_partial_fill() {
            let config = self.config.lock();
            config.partial_fill_qty.max(1).min(total_qty)
        } else {
            total_qty
        };

        Ok(OrderStatusResponse::new(
            query.execution_id.clone(),
            if filled_qty == total_qty { "filled" } else { "partially_filled" }.to_string(),
            "AAPL".to_string(),
            OrderSide::Buy,
            total_qty,
        ).with_fill(filled_qty, 150.0))
    }

    fn query_open_orders(&self) -> Result<Vec<OpenOrderInfo>, BrokerError> {
        Ok(self.open_orders.clone())
    }

    fn query_positions(&self) -> Result<Vec<PositionInfo>, BrokerError> {
        Ok(self.positions.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_execution_port_submit() {
        let port = MockExecutionPort::default();
        let cmd = OrderCommand {
            order_id: "test-1".to_string(),
            symbol: "AAPL".to_string(),
            side: OrderSide::Buy,
            quantity: 10.0,
            order_type: OrderType::Limit,
            limit_price: Some(150.0),
            stop_price: None,
            time_in_force: TimeInForce::Day,
            correlation_id: "corr-1".to_string(),
            trace_id: 42,
        };
        let result = port.submit_order(&cmd);
        assert!(result.is_ok());
        assert!(result.unwrap().contains("test-1"));
    }

    #[test]
    fn test_mock_execution_port_cancel() {
        let port = MockExecutionPort::default();
        let cmd = CancelCommand {
            execution_id: "test-1".to_string(),
        };
        let result = port.cancel_order(&cmd);
        assert!(result.is_ok());
    }

    #[test]
    fn test_order_status_response() {
        let response = OrderStatusResponse::new(
            "order-1".to_string(),
            "filled".to_string(),
            "AAPL".to_string(),
            OrderSide::Buy,
            100,
        ).with_fill(50, 150.0);

        assert_eq!(response.filled_qty, 50);
        assert_eq!(response.remaining_qty, 50);
        assert_eq!(response.avg_fill_price, Some(150.0));
    }

    #[test]
    fn test_mock_query_open_orders() {
        let mut port = MockExecutionPort::default();
        port.open_orders.push(OpenOrderInfo {
            order_id: "o1".to_string(),
            symbol: "AAPL".to_string(),
            side: OrderSide::Buy,
            quantity: 10.0,
            filled_qty: 0.0,
            status: "new".to_string(),
        });
        let orders = port.query_open_orders().unwrap();
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].symbol, "AAPL");
    }

    #[test]
    fn test_mock_query_positions() {
        let mut port = MockExecutionPort::default();
        port.positions.push(PositionInfo {
            symbol: "AAPL".to_string(),
            qty: 100.0,
            avg_entry_price: 150.0,
            current_price: 155.0,
        });
        let positions = port.query_positions().unwrap();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].symbol, "AAPL");
    }

    #[test]
    fn test_mock_explicit_failure() {
        let port = MockExecutionPort::builder()
            .explicit_failure(true)
            .failure_message("Test rejection".to_string())
            .build();

        let cmd = OrderCommand {
            order_id: "fail-1".to_string(),
            symbol: "AAPL".to_string(),
            side: OrderSide::Buy,
            quantity: 10.0,
            order_type: OrderType::Market,
            limit_price: None,
            stop_price: None,
            time_in_force: TimeInForce::Day,
            correlation_id: "corr-1".to_string(),
            trace_id: 42,
        };

        let result = port.submit_order(&cmd);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), BrokerError::Rejected("Test rejection".to_string()));
        assert_eq!(port.submit_count(), 1);
        assert_eq!(port.failure_count(), 1);
    }

    #[test]
    fn test_mock_failure_rate() {
        let port = MockExecutionPort::builder()
            .failure_rate(1.0) // 100% failure
            .build();

        let cmd = OrderCommand {
            order_id: "rate-fail".to_string(),
            symbol: "AAPL".to_string(),
            side: OrderSide::Buy,
            quantity: 10.0,
            order_type: OrderType::Market,
            limit_price: None,
            stop_price: None,
            time_in_force: TimeInForce::Day,
            correlation_id: "corr-1".to_string(),
            trace_id: 42,
        };

        // With 100% failure rate, all submissions should fail
        for i in 0..10 {
            let result = port.submit_order(&cmd);
            assert!(result.is_err(), "Submission {} should fail with 100% failure rate", i);
        }
        assert_eq!(port.submit_count(), 10);
        assert_eq!(port.failure_count(), 10);
    }

    #[test]
    fn test_mock_latency() {
        let port = MockExecutionPort::builder()
            .latency_ms(50)
            .build();

        let cmd = OrderCommand {
            order_id: "latency-test".to_string(),
            symbol: "AAPL".to_string(),
            side: OrderSide::Buy,
            quantity: 10.0,
            order_type: OrderType::Market,
            limit_price: None,
            stop_price: None,
            time_in_force: TimeInForce::Day,
            correlation_id: "corr-1".to_string(),
            trace_id: 42,
        };

        let start = std::time::Instant::now();
        let _ = port.submit_order(&cmd);
        let elapsed = start.elapsed();

        assert!(elapsed >= Duration::from_millis(45), "Should have latency of at least 45ms");
    }

    #[test]
    fn test_mock_partial_fill() {
        let port = MockExecutionPort::builder()
            .partial_fill_rate(1.0) // 100% partial fills
            .partial_fill_qty(50)
            .build();

        let query = StatusQuery {
            execution_id: "partial-fill-test".to_string(),
        };

        let result = port.get_order_status(&query).unwrap();
        assert_eq!(result.status, "partially_filled");
        assert_eq!(result.filled_qty, 50);
        assert_eq!(result.remaining_qty, 50);
    }

    #[test]
    fn test_mock_config_update() {
        let port = MockExecutionPort::default();
        assert_eq!(port.config().failure_rate, 0.0);

        port.update_config(MockConfig {
            failure_rate: 0.5,
            latency_ms: 100,
            ..Default::default()
        });

        assert_eq!(port.config().failure_rate, 0.5);
        assert_eq!(port.config().latency_ms, 100);
    }

    #[test]
    fn test_mock_builder_fluent() {
        let port = MockExecutionPort::builder()
            .failure_rate(0.25)
            .latency_ms(10)
            .partial_fill_rate(0.3)
            .partial_fill_qty(25)
            .explicit_failure(false)
            .failure_message("Builder test".to_string())
            .build();

        let config = port.config();
        assert_eq!(config.failure_rate, 0.25);
        assert_eq!(config.latency_ms, 10);
        assert_eq!(config.partial_fill_rate, 0.3);
        assert_eq!(config.partial_fill_qty, 25);
        assert!(!config.explicit_failure);
        assert_eq!(config.failure_message, "Builder test");
    }
}
