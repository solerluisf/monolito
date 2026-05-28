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
    /// Trace ID propagated from RawTick for causal tracing.
    pub trace_id: u64,
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
            trace_id: 0,
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

    pub fn with_trace_id(mut self, trace_id: u64) -> Self {
        self.trace_id = trace_id;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_lifecycle_event_trace_id() {
        let trace_id = 54321u64;
        let event = OrderLifecycleEvent::new(
            "exec-123".to_string(),
            "AAPL".to_string(),
            OrderLifecycleEventType::Submitted,
            1000,
        )
        .with_trace_id(trace_id);

        assert_eq!(event.trace_id, trace_id);
    }

    #[test]
    fn test_order_lifecycle_event_trace_id_filled() {
        let trace_id = 98765u64;
        let event = OrderLifecycleEvent::new(
            "exec-456".to_string(),
            "TSLA".to_string(),
            OrderLifecycleEventType::Filled,
            2000,
        )
        .with_fill(10.0, 150.0)
        .with_trace_id(trace_id);

        assert_eq!(event.trace_id, trace_id);
        assert_eq!(event.filled_quantity, 10.0);
        assert_eq!(event.fill_price, 150.0);
    }

    #[test]
    fn test_order_lifecycle_event_trace_id_default_zero() {
        let event = OrderLifecycleEvent::new(
            "exec-789".to_string(),
            "MSFT".to_string(),
            OrderLifecycleEventType::Cancelled,
            3000,
        );

        assert_eq!(event.trace_id, 0);
    }

    #[test]
    fn test_order_lifecycle_event_is_terminal() {
        let event = OrderLifecycleEvent::new(
            "exec-001".to_string(),
            "AAPL".to_string(),
            OrderLifecycleEventType::Filled,
            1000,
        );
        assert!(event.is_terminal());

        let event = OrderLifecycleEvent::new(
            "exec-002".to_string(),
            "AAPL".to_string(),
            OrderLifecycleEventType::Submitted,
            1000,
        );
        assert!(!event.is_terminal());
    }
}
