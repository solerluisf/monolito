use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::order_state_machine::{OrderStateMachine, OrderState, OrderEvent};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderStatus {
    Pending,
    Submitted,
    PartiallyFilled { filled_qty: f64 },
    Filled,
    Cancelled,
    Rejected { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub order_id: String,
    pub symbol: String,
    pub side: String,
    pub quantity: f64,
    pub filled_quantity: f64,
    pub limit_price: Option<f64>,
    pub status: OrderStatus,
    pub submitted_ns: u64,
    pub correlation_id: String,
}

pub struct OrderTracker {
    pub orders: HashMap<String, Order>,
    pub state_machines: HashMap<String, OrderStateMachine>,
    pub orders_by_symbol: HashMap<String, Vec<String>>,
}

impl OrderTracker {
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
            state_machines: HashMap::new(),
            orders_by_symbol: HashMap::new(),
        }
    }

    pub fn add_order(&mut self, order: Order) {
        self.orders_by_symbol
            .entry(order.symbol.clone())
            .or_default()
            .push(order.order_id.clone());
        self.orders.insert(order.order_id.clone(), order);
    }

    pub fn create_order(
        &mut self,
        symbol: &str,
        side: &str,
        quantity: f64,
        limit_price: Option<f64>,
        correlation_id: &str,
    ) -> String {
        let order_id = uuid::Uuid::new_v4().to_string();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let order = Order {
            order_id: order_id.clone(),
            symbol: symbol.to_string(),
            side: side.to_string(),
            quantity,
            filled_quantity: 0.0,
            limit_price,
            status: OrderStatus::Pending,
            submitted_ns: now,
            correlation_id: correlation_id.to_string(),
        };

        self.add_order(order);

        let sm = OrderStateMachine::new();
        self.state_machines.insert(order_id.clone(), sm);

        order_id
    }

    pub fn transition_order(
        &mut self,
        order_id: &str,
        event: OrderEvent,
        timestamp_ns: u64,
    ) -> Result<OrderState, String> {
        let sm = self.state_machines
            .get_mut(order_id)
            .ok_or_else(|| format!("Order {} not found", order_id))?;

        let new_state = sm.apply_event(event, timestamp_ns)
            .map_err(|e| e.to_string())?;

        let total_filled = sm.metadata().total_filled_qty;
        let rejection_reason = sm.metadata().rejection_reason.clone();

        if let Some(order) = self.orders.get_mut(order_id) {
            order.status = match &new_state {
                OrderState::Pending => OrderStatus::Pending,
                OrderState::Submitted => OrderStatus::Submitted,
                OrderState::PartiallyFilled => OrderStatus::PartiallyFilled {
                    filled_qty: total_filled,
                },
                OrderState::Filled => OrderStatus::Filled,
                OrderState::Cancelled => OrderStatus::Cancelled,
                OrderState::Rejected => OrderStatus::Rejected {
                    reason: rejection_reason.unwrap_or_default(),
                },
                OrderState::Expired => OrderStatus::Cancelled,
                OrderState::Replaced => OrderStatus::Cancelled,
            };
            order.filled_quantity = total_filled;
        }

        Ok(new_state)
    }

    pub fn submit_order(&mut self, order_id: &str, timestamp_ns: u64) -> Result<OrderState, String> {
        self.transition_order(order_id, OrderEvent::Submit, timestamp_ns)
    }

    pub fn partial_fill_order(&mut self, order_id: &str, filled_qty: f64, fill_price: f64, timestamp_ns: u64) -> Result<OrderState, String> {
        self.transition_order(order_id, OrderEvent::PartialFill { filled_qty, fill_price }, timestamp_ns)
    }

    pub fn full_fill_order(&mut self, order_id: &str, filled_qty: f64, fill_price: f64, timestamp_ns: u64) -> Result<OrderState, String> {
        self.transition_order(order_id, OrderEvent::FullFill { filled_qty, fill_price }, timestamp_ns)
    }

    pub fn cancel_order(&mut self, order_id: &str, timestamp_ns: u64) -> Result<OrderState, String> {
        self.transition_order(order_id, OrderEvent::Cancel, timestamp_ns)
    }

    pub fn reject_order(&mut self, order_id: &str, reason: String, timestamp_ns: u64) -> Result<OrderState, String> {
        self.transition_order(order_id, OrderEvent::Reject { reason }, timestamp_ns)
    }

    pub fn expire_order(&mut self, order_id: &str, timestamp_ns: u64) -> Result<OrderState, String> {
        self.transition_order(order_id, OrderEvent::Expire, timestamp_ns)
    }

    pub fn replace_order(&mut self, order_id: &str, timestamp_ns: u64) -> Result<OrderState, String> {
        self.transition_order(order_id, OrderEvent::Replace, timestamp_ns)
    }

    #[deprecated(note = "Use transition_order with explicit events instead")]
    pub fn update_status(&mut self, order_id: &str, status: OrderStatus) {
        if let Some(order) = self.orders.get_mut(order_id) {
            order.status = status;
        }
    }

    #[deprecated(note = "Use full_fill_order or partial_fill_order instead")]
    pub fn update_fill(&mut self, order_id: &str, filled_qty: f64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        if let Some(order) = self.orders.get(order_id) {
            let fill_price = order.limit_price.unwrap_or(0.0);
            if filled_qty >= order.quantity {
                let _ = self.full_fill_order(order_id, order.quantity, fill_price, now);
            } else {
                let _ = self.partial_fill_order(order_id, filled_qty, fill_price, now);
            }
        }
    }

    pub fn get_order(&self, order_id: &str) -> Option<&Order> {
        self.orders.get(order_id)
    }

    pub fn get_state_machine(&self, order_id: &str) -> Option<&OrderStateMachine> {
        self.state_machines.get(order_id)
    }

    pub fn get_orders_by_symbol(&self, symbol: &str) -> Vec<&Order> {
        self.orders_by_symbol
            .get(symbol)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.orders.get(id))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn open_orders_count(&self) -> usize {
        self.orders.values().filter(|o| {
            matches!(o.status, OrderStatus::Submitted | OrderStatus::PartiallyFilled { .. })
        }).count()
    }

    pub fn is_order_terminal(&self, order_id: &str) -> bool {
        self.state_machines
            .get(order_id)
            .map(|sm| sm.is_terminal())
            .unwrap_or(false)
    }

    pub fn is_order_active(&self, order_id: &str) -> bool {
        self.state_machines
            .get(order_id)
            .map(|sm| sm.is_active())
            .unwrap_or(false)
    }
}

impl Default for OrderTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_tracker_create() {
        let mut tracker = OrderTracker::new();
        let id = tracker.create_order("AAPL", "buy", 10.0, Some(150.0), "corr-1");
        assert!(!id.is_empty());
        assert_eq!(tracker.open_orders_count(), 0);
        assert!(tracker.is_order_active(&id));
        assert!(!tracker.is_order_terminal(&id));
    }

    #[test]
    fn test_order_tracker_valid_transition_submit() {
        let mut tracker = OrderTracker::new();
        let id = tracker.create_order("AAPL", "buy", 10.0, None, "corr-1");
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
        let result = tracker.submit_order(&id, now);
        assert!(result.is_ok());
        assert_eq!(tracker.open_orders_count(), 1);
        let order = tracker.get_order(&id).unwrap();
        assert!(matches!(order.status, OrderStatus::Submitted));
    }

    #[test]
    fn test_order_tracker_valid_transition_partial_fill() {
        let mut tracker = OrderTracker::new();
        let id = tracker.create_order("AAPL", "buy", 10.0, None, "corr-1");
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
        tracker.submit_order(&id, now).unwrap();
        let result = tracker.partial_fill_order(&id, 5.0, 150.0, now + 1_000_000);
        assert!(result.is_ok());
        let order = tracker.get_order(&id).unwrap();
        assert_eq!(order.filled_quantity, 5.0);
        assert!(matches!(order.status, OrderStatus::PartiallyFilled { .. }));
    }

    #[test]
    fn test_order_tracker_valid_transition_full_fill() {
        let mut tracker = OrderTracker::new();
        let id = tracker.create_order("AAPL", "buy", 10.0, None, "corr-1");
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
        tracker.submit_order(&id, now).unwrap();
        let result = tracker.full_fill_order(&id, 10.0, 150.0, now + 1_000_000);
        assert!(result.is_ok());
        let order = tracker.get_order(&id).unwrap();
        assert!(matches!(order.status, OrderStatus::Filled));
        assert_eq!(tracker.open_orders_count(), 0);
        assert!(tracker.is_order_terminal(&id));
    }

    #[test]
    fn test_order_tracker_invalid_transition_after_fill() {
        let mut tracker = OrderTracker::new();
        let id = tracker.create_order("AAPL", "buy", 10.0, None, "corr-1");
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
        tracker.submit_order(&id, now).unwrap();
        tracker.full_fill_order(&id, 10.0, 150.0, now + 1_000_000).unwrap();
        let result = tracker.submit_order(&id, now + 2_000_000);
        assert!(result.is_err());
    }

    #[test]
    fn test_order_tracker_invalid_transition_after_cancel() {
        let mut tracker = OrderTracker::new();
        let id = tracker.create_order("AAPL", "buy", 10.0, None, "corr-1");
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
        tracker.submit_order(&id, now).unwrap();
        tracker.cancel_order(&id, now + 1_000_000).unwrap();
        let result = tracker.partial_fill_order(&id, 5.0, 150.0, now + 2_000_000);
        assert!(result.is_err());
    }

    #[test]
    fn test_order_tracker_by_symbol() {
        let mut tracker = OrderTracker::new();
        tracker.create_order("AAPL", "buy", 10.0, None, "corr-1");
        tracker.create_order("AAPL", "sell", 5.0, None, "corr-2");
        tracker.create_order("MSFT", "buy", 3.0, None, "corr-3");
        assert_eq!(tracker.get_orders_by_symbol("AAPL").len(), 2);
        assert_eq!(tracker.get_orders_by_symbol("MSFT").len(), 1);
    }

    #[test]
    fn test_order_tracker_state_machine_metadata() {
        let mut tracker = OrderTracker::new();
        let id = tracker.create_order("AAPL", "buy", 10.0, None, "corr-1");
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
        tracker.submit_order(&id, now).unwrap();
        tracker.partial_fill_order(&id, 5.0, 100.0, now + 1_000_000).unwrap();
        tracker.partial_fill_order(&id, 5.0, 120.0, now + 2_000_000).unwrap();

        let sm = tracker.get_state_machine(&id).unwrap();
        assert_eq!(sm.metadata().total_filled_qty, 10.0);
        assert!((sm.metadata().avg_fill_price - 110.0).abs() < 0.01);
        assert_eq!(sm.transition_count(), 3);
    }
}
