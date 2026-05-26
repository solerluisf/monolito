use std::collections::HashMap;

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

pub trait IExecutionPort: Send + Sync {
    fn submit_order(&self, cmd: &OrderCommand) -> Result<String, String>;
    fn cancel_order(&self, cmd: &CancelCommand) -> Result<(), String>;
    fn replace_order(&self, cmd: &ReplaceCommand) -> Result<String, String>;
    fn get_order_status(&self, query: &StatusQuery) -> Result<OrderStatusResponse, String>;
    fn query_open_orders(&self) -> Result<Vec<OpenOrderInfo>, String>;
    fn query_positions(&self) -> Result<Vec<PositionInfo>, String>;
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
    pub fn new(api_key: &str, api_secret: &str, paper: bool) -> Result<Self, String> {
        let base_url = if paper {
            "https://paper-api.alpaca.markets/v2"
        } else {
            "https://api.alpaca.markets/v2"
        };

        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| format!("Failed to create HTTP client: {}", e))?;

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
    fn submit_order(&self, cmd: &OrderCommand) -> Result<String, String> {
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
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(format!("Order rejected: {} - {}", status, body));
        }

        let order: AlpacaOrderResponse = response.json()
            .map_err(|e| format!("Failed to parse response: {}", e))?;

        tracing::info!(order_id = %order.id, symbol = %order.symbol, "Order submitted to Alpaca");

        Ok(order.id)
    }

    fn cancel_order(&self, cmd: &CancelCommand) -> Result<(), String> {
        let url = format!("{}/orders/{}", self.base_url, cmd.execution_id);

        let response = self.client.delete(&url)
            .headers(self.get_headers_map())
            .send()
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(format!("Cancel failed: {} - {}", status, body));
        }

        tracing::info!(order_id = %cmd.execution_id, "Order cancelled on Alpaca");

        Ok(())
    }

    fn replace_order(&self, cmd: &ReplaceCommand) -> Result<String, String> {
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
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(format!("Replace failed: {} - {}", status, body));
        }

        let order: AlpacaOrderResponse = response.json()
            .map_err(|e| format!("Failed to parse response: {}", e))?;

        tracing::info!(order_id = %order.id, "Order replaced on Alpaca");

        Ok(order.id)
    }

    fn get_order_status(&self, query: &StatusQuery) -> Result<OrderStatusResponse, String> {
        let url = format!("{}/orders/{}", self.base_url, query.execution_id);

        let response = self.client.get(&url)
            .headers(self.get_headers_map())
            .send()
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(format!("Query failed: {} - {}", status, body));
        }

        let order: AlpacaOrderResponse = response.json()
            .map_err(|e| format!("Failed to parse response: {}", e))?;

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

    fn query_open_orders(&self) -> Result<Vec<OpenOrderInfo>, String> {
        let url = format!("{}/orders?status=open", self.base_url);

        let response = self.client.get(&url)
            .headers(self.get_headers_map())
            .send()
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(format!("Query open orders failed: {} - {}", status, body));
        }

        let orders: Vec<AlpacaOrderResponse> = response.json()
            .map_err(|e| format!("Failed to parse response: {}", e))?;

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

    fn query_positions(&self) -> Result<Vec<PositionInfo>, String> {
        let url = format!("{}/positions", self.base_url);

        let response = self.client.get(&url)
            .headers(self.get_headers_map())
            .send()
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(format!("Query positions failed: {} - {}", status, body));
        }

        let positions: Vec<AlpacaPosition> = response.json()
            .map_err(|e| format!("Failed to parse response: {}", e))?;

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
}

impl Default for MockExecutionPort {
    fn default() -> Self {
        Self {
            open_orders: Vec::new(),
            positions: Vec::new(),
        }
    }
}

impl IExecutionPort for MockExecutionPort {
    fn submit_order(&self, cmd: &OrderCommand) -> Result<String, String> {
        Ok(format!("mock-{}", cmd.order_id))
    }

    fn cancel_order(&self, _cmd: &CancelCommand) -> Result<(), String> {
        Ok(())
    }

    fn replace_order(&self, cmd: &ReplaceCommand) -> Result<String, String> {
        Ok(format!("mock-replaced-{}", cmd.execution_id))
    }

    fn get_order_status(&self, query: &StatusQuery) -> Result<OrderStatusResponse, String> {
        Ok(OrderStatusResponse::new(
            query.execution_id.clone(),
            "filled".to_string(),
            "AAPL".to_string(),
            OrderSide::Buy,
            100,
        ).with_fill(100, 150.0))
    }

    fn query_open_orders(&self) -> Result<Vec<OpenOrderInfo>, String> {
        Ok(self.open_orders.clone())
    }

    fn query_positions(&self) -> Result<Vec<PositionInfo>, String> {
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
}
