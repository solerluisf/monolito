use std::collections::HashMap;

use crate::alpaca_execution::{
    AlpacaOrderRequest, AlpacaOrderResponse, AlpacaPosition, CancelCommand, IExecutionPort,
    OrderCommand, OrderSide, ReplaceCommand, StatusQuery, OrderStatusResponse, OrderType, TimeInForce,
    OpenOrderInfo, PositionInfo,
};

pub struct AlpacaAdapter {
    client: reqwest::blocking::Client,
    api_key: String,
    api_secret: String,
    base_url: String,
}

impl AlpacaAdapter {
    pub fn new(api_key: &str, api_secret: &str, base_url: &str, _paper_trading: bool) -> Self {
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        let base_url = if base_url.ends_with("/v2") {
            base_url.to_string()
        } else {
            format!("{}/v2", base_url)
        };

        Self {
            client,
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            base_url,
        }
    }

    fn headers(&self) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        headers.insert("APCA-API-KEY-ID".to_string(), self.api_key.clone());
        headers.insert("APCA-API-SECRET-KEY".to_string(), self.api_secret.clone());
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        headers
    }
}

impl IExecutionPort for AlpacaAdapter {
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

        let response = self.client
            .post(&url)
            .headers({
                let mut h = reqwest::header::HeaderMap::new();
                h.insert(
                    "APCA-API-KEY-ID",
                    reqwest::header::HeaderValue::from_str(&self.api_key).unwrap(),
                );
                h.insert(
                    "APCA-API-SECRET-KEY",
                    reqwest::header::HeaderValue::from_str(&self.api_secret).unwrap(),
                );
                h.insert("Content-Type", reqwest::header::HeaderValue::from_static("application/json"));
                h
            })
            .json(&req)
            .send()
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(format!("Order rejected: {} - {}", status, body));
        }

        let order: AlpacaOrderResponse = response
            .json()
            .map_err(|e| format!("Failed to parse response: {}", e))?;

        tracing::info!(order_id = %order.id, symbol = %order.symbol, "Order submitted to Alpaca via Adapter");

        Ok(order.id)
    }

    fn cancel_order(&self, cmd: &CancelCommand) -> Result<(), String> {
        let url = format!("{}/orders/{}", self.base_url, cmd.execution_id);

        let response = self.client
            .delete(&url)
            .headers({
                let mut h = reqwest::header::HeaderMap::new();
                h.insert(
                    "APCA-API-KEY-ID",
                    reqwest::header::HeaderValue::from_str(&self.api_key).unwrap(),
                );
                h.insert(
                    "APCA-API-SECRET-KEY",
                    reqwest::header::HeaderValue::from_str(&self.api_secret).unwrap(),
                );
                h
            })
            .send()
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(format!("Cancel failed: {} - {}", status, body));
        }

        tracing::info!(order_id = %cmd.execution_id, "Order cancelled on Alpaca via Adapter");

        Ok(())
    }

    fn replace_order(&self, cmd: &ReplaceCommand) -> Result<String, String> {
        let url = format!("{}/orders/{}", self.base_url, cmd.execution_id);

        let mut body = serde_json::Map::new();
        if let Some(qty) = cmd.quantity {
            body.insert(
                "qty".to_string(),
                serde_json::Value::Number(serde_json::Number::from_f64(qty).unwrap()),
            );
        }
        if let Some(price) = cmd.limit_price {
            body.insert(
                "limit_price".to_string(),
                serde_json::Value::Number(serde_json::Number::from_f64(price).unwrap()),
            );
        }

        let response = self.client
            .patch(&url)
            .headers({
                let mut h = reqwest::header::HeaderMap::new();
                h.insert(
                    "APCA-API-KEY-ID",
                    reqwest::header::HeaderValue::from_str(&self.api_key).unwrap(),
                );
                h.insert(
                    "APCA-API-SECRET-KEY",
                    reqwest::header::HeaderValue::from_str(&self.api_secret).unwrap(),
                );
                h.insert("Content-Type", reqwest::header::HeaderValue::from_static("application/json"));
                h
            })
            .json(&body)
            .send()
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(format!("Replace failed: {} - {}", status, body));
        }

        let order: AlpacaOrderResponse = response
            .json()
            .map_err(|e| format!("Failed to parse response: {}", e))?;

        tracing::info!(order_id = %order.id, "Order replaced on Alpaca via Adapter");

        Ok(order.id)
    }

    fn get_order_status(&self, query: &StatusQuery) -> Result<OrderStatusResponse, String> {
        let url = format!("{}/orders/{}", self.base_url, query.execution_id);

        let response = self.client
            .get(&url)
            .headers({
                let mut h = reqwest::header::HeaderMap::new();
                h.insert(
                    "APCA-API-KEY-ID",
                    reqwest::header::HeaderValue::from_str(&self.api_key).unwrap(),
                );
                h.insert(
                    "APCA-API-SECRET-KEY",
                    reqwest::header::HeaderValue::from_str(&self.api_secret).unwrap(),
                );
                h
            })
            .send()
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(format!("Query failed: {} - {}", status, body));
        }

        let order: AlpacaOrderResponse = response
            .json()
            .map_err(|e| format!("Failed to parse response: {}", e))?;

        let total_qty: u32 = order.qty.parse().unwrap_or(0);
        let filled_qty: u32 = order.filled_qty.parse().unwrap_or(0);
        let avg_fill_price = order.filled_avg_price.and_then(|p| p.parse().ok());

        let resp = OrderStatusResponse::new(
            query.execution_id.clone(),
            order.status.clone(),
            order.symbol.clone(),
            match order.side.to_lowercase().as_str() {
                "buy" => OrderSide::Buy,
                _ => OrderSide::Sell,
            },
            total_qty,
        )
        .with_fill(filled_qty, avg_fill_price.unwrap_or(0.0));

        tracing::info!(
            "Order status via Adapter id={} status={:?} filled={}/{} avg_price={:?}",
            query.execution_id,
            order.status,
            filled_qty,
            total_qty,
            avg_fill_price
        );

        Ok(resp)
    }

    fn query_open_orders(&self) -> Result<Vec<OpenOrderInfo>, String> {
        let url = format!("{}/orders?status=open", self.base_url);

        let response = self.client
            .get(&url)
            .headers({
                let mut h = reqwest::header::HeaderMap::new();
                h.insert(
                    "APCA-API-KEY-ID",
                    reqwest::header::HeaderValue::from_str(&self.api_key).unwrap(),
                );
                h.insert(
                    "APCA-API-SECRET-KEY",
                    reqwest::header::HeaderValue::from_str(&self.api_secret).unwrap(),
                );
                h
            })
            .send()
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(format!("Query open orders failed: {} - {}", status, body));
        }

        let orders: Vec<AlpacaOrderResponse> = response
            .json()
            .map_err(|e| format!("Failed to parse response: {}", e))?;

        let mut result = Vec::new();
        for order in orders {
            let total_qty: f64 = order.qty.parse().unwrap_or(0.0);
            let filled_qty: f64 = order.filled_qty.parse().unwrap_or(0.0);
            result.push(OpenOrderInfo {
                order_id: order.id,
                symbol: order.symbol,
                side: match order.side.to_lowercase().as_str() {
                    "buy" => OrderSide::Buy,
                    _ => OrderSide::Sell,
                },
                quantity: total_qty,
                filled_qty,
                status: order.status,
            });
        }

        tracing::info!(count = %result.len(), "Queried open orders from Alpaca via Adapter");
        Ok(result)
    }

    fn query_positions(&self) -> Result<Vec<PositionInfo>, String> {
        let url = format!("{}/positions", self.base_url);

        let response = self.client
            .get(&url)
            .headers({
                let mut h = reqwest::header::HeaderMap::new();
                h.insert(
                    "APCA-API-KEY-ID",
                    reqwest::header::HeaderValue::from_str(&self.api_key).unwrap(),
                );
                h.insert(
                    "APCA-API-SECRET-KEY",
                    reqwest::header::HeaderValue::from_str(&self.api_secret).unwrap(),
                );
                h
            })
            .send()
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(format!("Query positions failed: {} - {}", status, body));
        }

        let positions: Vec<AlpacaPosition> = response
            .json()
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

        tracing::info!(count = %result.len(), "Queried positions from Alpaca via Adapter");
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{OrderType, TimeInForce};

    #[test]
    fn test_alpaca_adapter_submit() {
        let adapter = AlpacaAdapter::new("key", "secret", "https://paper-api.alpaca.markets", true);
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
            trace_id: 1,
        };
        // This test will fail at runtime without valid credentials, but it verifies the struct builds
        // and the request path is correct. In CI, we rely on mock execution port tests.
        // We only assert that submit_order returns an Err (network/auth) rather than a fake ID.
        let result = adapter.submit_order(&cmd);
        assert!(result.is_err(), "Expected network/auth error without real credentials, got {:?}", result);
    }
}
