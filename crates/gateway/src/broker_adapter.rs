use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BrokerType {
    Alpaca,
    Mock,
    Replay,
}

#[derive(Debug, Clone)]
pub struct MarketDataEvent {
    pub symbol: String,
    pub timestamp_ns: u64,
    pub bid: f64,
    pub ask: f64,
    pub bid_size: u64,
    pub ask_size: u64,
    pub last_price: f64,
    pub last_size: u64,
}

pub struct BrokerAdapterFactory;

impl BrokerAdapterFactory {
    pub fn create(broker_type: &str) -> Box<dyn crate::IExecutionPort> {
        match broker_type {
            "alpaca" => match crate::AlpacaExecutionPort::new("", "", true) {
                Ok(port) => Box::new(port),
                Err(_) => Box::new(crate::MockExecutionPort),
            },
            "mock" | _ => Box::new(crate::MockExecutionPort),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IExecutionPort, OrderCommand, OrderSide, OrderType, TimeInForce};

    #[test]
    fn test_broker_adapter_factory_mock() {
        let adapter = BrokerAdapterFactory::create("mock");
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
    }
}
