use serde::{Deserialize, Serialize};
use unified_trading_core::symbol_registry::next_request_id;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderLifecycleEventType {
    Submitted,
    PartialFill,
    Filled,
    Rejected,
    Cancelled,
    Replaced,
    Expired,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderLifecycleEvent {
    pub event_id: String,
    pub execution_id: String,
    pub client_order_id: Option<String>,
    pub symbol: String,
    pub event_type: OrderLifecycleEventType,
    pub timestamp_ns: u64,
    pub filled_quantity: f64,
    pub fill_price: f64,
    pub remaining_quantity: f64,
    pub raw_status: String,
}

impl OrderLifecycleEvent {
    pub fn new(
        execution_id: String,
        symbol: String,
        event_type: OrderLifecycleEventType,
        timestamp_ns: u64,
    ) -> Self {
        Self {
            event_id: next_request_id().to_string(),
            execution_id,
            client_order_id: None,
            symbol,
            event_type,
            timestamp_ns,
            filled_quantity: 0.0,
            fill_price: 0.0,
            remaining_quantity: 0.0,
            raw_status: String::new(),
        }
    }

    pub fn with_fill(mut self, quantity: f64, price: f64) -> Self {
        self.filled_quantity = quantity;
        self.fill_price = price;
        self
    }

    pub fn with_remaining(mut self, quantity: f64) -> Self {
        self.remaining_quantity = quantity;
        self
    }

    pub fn with_status(mut self, status: String) -> Self {
        self.raw_status = status;
        self
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.event_type,
            OrderLifecycleEventType::Filled
                | OrderLifecycleEventType::Rejected
                | OrderLifecycleEventType::Cancelled
                | OrderLifecycleEventType::Expired
        )
    }
}
