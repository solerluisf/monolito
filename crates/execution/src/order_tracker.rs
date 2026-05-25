use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

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
    pub orders_by_symbol: HashMap<String, Vec<String>>,
}

impl OrderTracker {
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
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

    pub fn update_status(&mut self, order_id: &str, status: OrderStatus) {
        if let Some(order) = self.orders.get_mut(order_id) {
            order.status = status;
        }
    }

    pub fn update_fill(&mut self, order_id: &str, filled_qty: f64) {
        if let Some(order) = self.orders.get_mut(order_id) {
            order.filled_quantity = filled_qty;
            if filled_qty >= order.quantity {
                order.status = OrderStatus::Filled;
            } else {
                order.status = OrderStatus::PartiallyFilled { filled_qty };
            }
        }
    }

    pub fn get_order(&self, order_id: &str) -> Option<&Order> {
        self.orders.get(order_id)
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
        self.orders.values().filter(|o| matches!(o.status, OrderStatus::Submitted | OrderStatus::PartiallyFilled { .. })).count()
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
        order_id
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
    }

    #[test]
    fn test_order_tracker_update_status() {
        let mut tracker = OrderTracker::new();
        let id = tracker.create_order("AAPL", "buy", 10.0, None, "corr-1");
        tracker.update_status(&id, OrderStatus::Submitted);
        assert_eq!(tracker.open_orders_count(), 1);
    }

    #[test]
    fn test_order_tracker_update_fill() {
        let mut tracker = OrderTracker::new();
        let id = tracker.create_order("AAPL", "buy", 10.0, None, "corr-1");
        tracker.update_status(&id, OrderStatus::Submitted);
        tracker.update_fill(&id, 5.0);
        let order = tracker.get_order(&id).unwrap();
        assert_eq!(order.filled_quantity, 5.0);
        assert!(matches!(order.status, OrderStatus::PartiallyFilled { .. }));
    }

    #[test]
    fn test_order_tracker_full_fill() {
        let mut tracker = OrderTracker::new();
        let id = tracker.create_order("AAPL", "buy", 10.0, None, "corr-1");
        tracker.update_status(&id, OrderStatus::Submitted);
        tracker.update_fill(&id, 10.0);
        let order = tracker.get_order(&id).unwrap();
        assert!(matches!(order.status, OrderStatus::Filled));
        assert_eq!(tracker.open_orders_count(), 0);
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
}
