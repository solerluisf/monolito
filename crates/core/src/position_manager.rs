use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Position {
    pub symbol: String,
    pub quantity: f64,
    pub avg_entry_price: f64,
    pub current_price: f64,
    pub realized_pnl: f64,
    pub total_bought: f64,
    pub total_sold: f64,
}

impl Position {
    pub fn new(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
            quantity: 0.0,
            avg_entry_price: 0.0,
            current_price: 0.0,
            realized_pnl: 0.0,
            total_bought: 0.0,
            total_sold: 0.0,
        }
    }

    pub fn unrealized_pnl(&self) -> f64 {
        if self.quantity == 0.0 {
            return 0.0;
        }
        (self.current_price - self.avg_entry_price) * self.quantity
    }

    pub fn total_pnl(&self) -> f64 {
        self.realized_pnl + self.unrealized_pnl()
    }

    pub fn market_value(&self) -> f64 {
        self.current_price * self.quantity
    }

    pub fn is_flat(&self) -> bool {
        self.quantity.abs() < 0.0001
    }
}

pub struct PositionManager {
    positions: Mutex<HashMap<String, Position>>,
}

impl PositionManager {
    pub fn new() -> Self {
        Self {
            positions: Mutex::new(HashMap::new()),
        }
    }

    pub fn on_fill(&self, symbol: &str, quantity: f64, price: f64, is_buy: bool) {
        let mut positions = self.positions.lock().unwrap();
        let position = positions
            .entry(symbol.to_string())
            .or_insert_with(|| Position::new(symbol));

        position.current_price = price;

        if is_buy {
            if position.quantity >= 0.0 {
                let total_cost = position.avg_entry_price * position.quantity + price * quantity;
                position.quantity += quantity;
                if position.quantity > 0.0 {
                    position.avg_entry_price = total_cost / position.quantity;
                }
                position.total_bought += price * quantity;
            } else {
                let close_qty = quantity.min(-position.quantity);
                let remaining = quantity - close_qty;

                if close_qty > 0.0 {
                    position.realized_pnl += (position.avg_entry_price - price) * close_qty;
                    position.quantity += close_qty;
                    position.total_sold += price * close_qty;
                }

                if remaining > 0.0 {
                    position.quantity += remaining;
                    position.avg_entry_price = price;
                    position.total_bought += price * remaining;
                }
            }
        } else {
            if position.quantity <= 0.0 {
                let total_cost = position.avg_entry_price * position.quantity.abs() + price * quantity;
                position.quantity -= quantity;
                if position.quantity < 0.0 {
                    position.avg_entry_price = total_cost / position.quantity.abs();
                }
                position.total_sold += price * quantity;
            } else {
                let close_qty = quantity.min(position.quantity);
                let remaining = quantity - close_qty;

                if close_qty > 0.0 {
                    position.realized_pnl += (price - position.avg_entry_price) * close_qty;
                    position.quantity -= close_qty;
                    position.total_sold += price * close_qty;
                }

                if remaining > 0.0 {
                    position.quantity -= remaining;
                    position.avg_entry_price = price;
                    position.total_bought += price * remaining;
                }
            }
        }

        if position.is_flat() {
            position.avg_entry_price = 0.0;
        }
    }

    pub fn update_price(&self, symbol: &str, price: f64) {
        let mut positions = self.positions.lock().unwrap();
        if let Some(position) = positions.get_mut(symbol) {
            position.current_price = price;
        }
    }

    pub fn get_position(&self, symbol: &str) -> Option<Position> {
        self.positions.lock().unwrap().get(symbol).cloned()
    }

    pub fn get_all_positions(&self) -> Vec<Position> {
        self.positions.lock().unwrap().values().cloned().collect()
    }

    pub fn total_unrealized_pnl(&self) -> f64 {
        self.positions.lock().unwrap()
            .values()
            .map(|p| p.unrealized_pnl())
            .sum()
    }

    pub fn total_realized_pnl(&self) -> f64 {
        self.positions.lock().unwrap()
            .values()
            .map(|p| p.realized_pnl)
            .sum()
    }

    pub fn total_market_value(&self) -> f64 {
        self.positions.lock().unwrap()
            .values()
            .map(|p| p.market_value())
            .sum()
    }

    pub fn position_count(&self) -> usize {
        self.positions.lock().unwrap()
            .values()
            .filter(|p| !p.is_flat())
            .count()
    }
}

impl Default for PositionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_position_initial_state() {
        let pm = PositionManager::new();
        assert!(pm.get_position("AAPL").is_none());
        assert_eq!(pm.total_unrealized_pnl(), 0.0);
        assert_eq!(pm.position_count(), 0);
    }

    #[test]
    fn test_buy_creates_position() {
        let pm = PositionManager::new();
        pm.on_fill("AAPL", 100.0, 150.0, true);
        let pos = pm.get_position("AAPL").unwrap();
        assert_eq!(pos.quantity, 100.0);
        assert_eq!(pos.avg_entry_price, 150.0);
        assert!(!pm.get_position("AAPL").unwrap().is_flat());
    }

    #[test]
    fn test_sell_reduces_position() {
        let pm = PositionManager::new();
        pm.on_fill("AAPL", 100.0, 150.0, true);
        pm.on_fill("AAPL", 50.0, 160.0, false);
        let pos = pm.get_position("AAPL").unwrap();
        assert_eq!(pos.quantity, 50.0);
        assert!(pos.realized_pnl > 0.0);
    }

    #[test]
    fn test_full_round_trip() {
        let pm = PositionManager::new();
        pm.on_fill("AAPL", 100.0, 150.0, true);
        pm.on_fill("AAPL", 100.0, 160.0, false);
        let pos = pm.get_position("AAPL").unwrap();
        assert!(pos.is_flat());
        assert_eq!(pos.realized_pnl, 1000.0);
    }

    #[test]
    fn test_unrealized_pnl() {
        let pm = PositionManager::new();
        pm.on_fill("AAPL", 100.0, 150.0, true);
        pm.update_price("AAPL", 155.0);
        let pos = pm.get_position("AAPL").unwrap();
        assert_eq!(pos.unrealized_pnl(), 500.0);
    }
}
