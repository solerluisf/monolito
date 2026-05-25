use crate::alpaca_execution::{IExecutionPort, OrderCommand, OrderSide, CancelCommand, ReplaceCommand, StatusQuery, OrderStatusResponse};

pub struct AlpacaAdapter {
    pub api_key: String,
    pub api_secret: String,
    pub base_url: String,
    pub paper_trading: bool,
}

impl AlpacaAdapter {
    pub fn new(api_key: &str, api_secret: &str, base_url: &str, paper_trading: bool) -> Self {
        Self {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            base_url: base_url.to_string(),
            paper_trading,
        }
    }
}

impl IExecutionPort for AlpacaAdapter {
    fn submit_order(&self, command: &OrderCommand) -> Result<String, String> {
        Ok(format!("alpaca-{}", command.order_id))
    }

    fn cancel_order(&self, cmd: &CancelCommand) -> Result<(), String> {
        let _ = cmd;
        Ok(())
    }

    fn replace_order(&self, cmd: &ReplaceCommand) -> Result<String, String> {
        Ok(format!("alpaca-replaced-{}", cmd.execution_id))
    }

    fn get_order_status(&self, query: &StatusQuery) -> Result<OrderStatusResponse, String> {
        Ok(OrderStatusResponse::new(
            query.execution_id.clone(),
            "submitted".to_string(),
            "AAPL".to_string(),
            OrderSide::Buy,
            100,
        ))
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
        };
        let result = adapter.submit_order(&cmd);
        assert!(result.is_ok());
        assert!(result.unwrap().starts_with("alpaca-"));
    }
}
