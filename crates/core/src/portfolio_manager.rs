use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub net_position: f64,
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
            net_position: 0.0,
            avg_entry_price: 0.0,
            current_price: 0.0,
            realized_pnl: 0.0,
            total_bought: 0.0,
            total_sold: 0.0,
        }
    }

    pub fn unrealized_pnl(&self) -> f64 {
        if self.net_position == 0.0 {
            return 0.0;
        }
        (self.current_price - self.avg_entry_price) * self.net_position
    }

    pub fn market_value(&self) -> f64 {
        self.current_price * self.net_position
    }

    pub fn is_flat(&self) -> bool {
        self.net_position.abs() < 0.0001
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioMetrics {
    pub total_exposure: f64,
    pub net_exposure: f64,
    pub gross_exposure: f64,
    pub leverage: f64,
    pub total_unrealized_pnl: f64,
    pub total_realized_pnl: f64,
    pub peak_equity: f64,
    pub current_equity: f64,
    pub drawdown_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenOrder {
    pub order_id: String,
    pub symbol: String,
    pub side: String,
    pub quantity: f64,
    pub filled_quantity: f64,
    pub limit_price: Option<f64>,
    pub status: String,
}

pub struct PortfolioManager {
    state: RwLock<PortfolioState>,
}

struct PortfolioState {
    positions: HashMap<String, Position>,
    open_orders: HashMap<String, OpenOrder>,
    peak_equity: f64,
    current_equity: f64,
    initial_equity: f64,
    flat_threshold: f64,
}

impl PortfolioManager {
    pub fn new(initial_equity: f64, flat_threshold: f64) -> Self {
        Self {
            state: RwLock::new(PortfolioState {
                positions: HashMap::new(),
                open_orders: HashMap::new(),
                peak_equity: initial_equity,
                current_equity: initial_equity,
                initial_equity,
                flat_threshold,
            }),
        }
    }

    /// Update position state from a fill event. This is the canonical writer path.
    pub fn on_fill(&self, symbol: &str, price: f64, quantity: f64, is_buy: bool) {
        let mut state = self.state.write();
        let pos = state.positions.entry(symbol.to_string()).or_insert_with(|| Position::new(symbol));

        let delta = if is_buy { quantity } else { -quantity };

        if (pos.net_position >= 0.0 && delta > 0.0) || (pos.net_position <= 0.0 && delta < 0.0) {
            // Adding to existing direction
            let total_qty = pos.net_position.abs() + delta.abs();
            pos.avg_entry_price = (pos.avg_entry_price * pos.net_position.abs() + price * delta.abs()) / total_qty;
        } else {
            // Closing or flipping direction
            let closed_qty = delta.abs().min(pos.net_position.abs());
            let pnl = closed_qty * (price - pos.avg_entry_price) * pos.net_position.signum();
            pos.realized_pnl += pnl;
        }

        pos.net_position += delta;
        pos.current_price = price;

        if is_buy {
            pos.total_bought += price * quantity;
        } else {
            pos.total_sold += price * quantity;
        }

        if pos.is_flat() {
            pos.avg_entry_price = 0.0;
        }

        drop(state);
        self.recalculate_equity();
    }

    pub fn update_price(&self, symbol: &str, price: f64) {
        let mut state = self.state.write();
        if let Some(pos) = state.positions.get_mut(symbol) {
            pos.current_price = price;
        }
        drop(state);
        self.recalculate_equity();
    }

    fn recalculate_equity(&self) {
        let mut state = self.state.write();
        let unrealized: f64 = state.positions.values().map(|p| {
            p.net_position * (p.current_price - p.avg_entry_price)
        }).sum();

        let realized: f64 = state.positions.values().map(|p| p.realized_pnl).sum();

        state.current_equity = state.initial_equity + unrealized + realized;

        if state.current_equity > state.peak_equity {
            state.peak_equity = state.current_equity;
        }
    }

    pub fn get_metrics(&self) -> PortfolioMetrics {
        let state = self.state.read();
        let gross: f64 = state.positions.values().map(|p| p.net_position.abs() * p.current_price).sum();
        let net: f64 = state.positions.values().map(|p| p.net_position * p.current_price).sum();
        let unrealized: f64 = state.positions.values().map(|p| p.unrealized_pnl()).sum();
        let realized: f64 = state.positions.values().map(|p| p.realized_pnl).sum();

        let drawdown = if state.peak_equity > 0.0 {
            (state.peak_equity - state.current_equity) / state.peak_equity * 100.0
        } else {
            0.0
        };

        let leverage = if state.current_equity > 0.0 {
            gross / state.current_equity
        } else {
            0.0
        };

        PortfolioMetrics {
            total_exposure: gross,
            net_exposure: net,
            gross_exposure: gross,
            leverage,
            total_unrealized_pnl: unrealized,
            total_realized_pnl: realized,
            peak_equity: state.peak_equity,
            current_equity: state.current_equity,
            drawdown_pct: drawdown,
        }
    }

    pub fn get_position(&self, symbol: &str) -> Option<Position> {
        self.state.read().positions.get(symbol).cloned()
    }

    pub fn get_all_positions(&self) -> Vec<Position> {
        self.state.read().positions.values().cloned().collect()
    }

    pub fn total_unrealized_pnl(&self) -> f64 {
        self.state.read().positions.values().map(|p| p.unrealized_pnl()).sum()
    }

    pub fn total_realized_pnl(&self) -> f64 {
        self.state.read().positions.values().map(|p| p.realized_pnl).sum()
    }

    pub fn total_market_value(&self) -> f64 {
        self.state.read().positions.values().map(|p| p.market_value()).sum()
    }

    pub fn position_count(&self) -> usize {
        self.state.read().positions.values().filter(|p| !p.is_flat()).count()
    }

    pub fn net_position(&self, symbol: &str) -> f64 {
        self.state.read().positions.get(symbol).map(|p| p.net_position).unwrap_or(0.0)
    }
}

impl Default for PortfolioManager {
    fn default() -> Self {
        Self::new(100_000.0, 0.001)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state() {
        let pm = PortfolioManager::new(100_000.0, 0.001);
        assert!(pm.get_position("AAPL").is_none());
        assert_eq!(pm.total_unrealized_pnl(), 0.0);
        assert_eq!(pm.position_count(), 0);
    }

    #[test]
    fn test_buy_creates_position() {
        let pm = PortfolioManager::new(100_000.0, 0.001);
        pm.on_fill("AAPL", 150.0, 100.0, true);
        let pos = pm.get_position("AAPL").unwrap();
        assert_eq!(pos.net_position, 100.0);
        assert!((pos.avg_entry_price - 150.0).abs() < 0.01);
        assert!(!pos.is_flat());
    }

    #[test]
    fn test_sell_reduces_position() {
        let pm = PortfolioManager::new(100_000.0, 0.001);
        pm.on_fill("AAPL", 150.0, 100.0, true);
        pm.on_fill("AAPL", 160.0, 50.0, false);
        let pos = pm.get_position("AAPL").unwrap();
        assert_eq!(pos.net_position, 50.0);
        assert!(pos.realized_pnl > 0.0);
    }

    #[test]
    fn test_full_round_trip() {
        let pm = PortfolioManager::new(100_000.0, 0.001);
        pm.on_fill("AAPL", 150.0, 100.0, true);
        pm.on_fill("AAPL", 160.0, 100.0, false);
        let pos = pm.get_position("AAPL").unwrap();
        assert!(pos.is_flat());
        assert_eq!(pos.realized_pnl, 1000.0);
    }

    #[test]
    fn test_unrealized_pnl() {
        let pm = PortfolioManager::new(100_000.0, 0.001);
        pm.on_fill("AAPL", 150.0, 100.0, true);
        pm.update_price("AAPL", 155.0);
        let pos = pm.get_position("AAPL").unwrap();
        assert_eq!(pos.unrealized_pnl(), 500.0);
    }

    #[test]
    fn test_portfolio_metrics() {
        let pm = PortfolioManager::new(100_000.0, 0.001);
        pm.on_fill("AAPL", 150.0, 10.0, true);
        let metrics = pm.get_metrics();
        assert!(metrics.gross_exposure > 0.0);
        assert!(metrics.leverage >= 0.0);
    }

    #[test]
    fn test_drawdown() {
        let pm = PortfolioManager::new(100_000.0, 0.001);
        pm.on_fill("AAPL", 150.0, 100.0, true);
        pm.update_price("AAPL", 140.0);
        let metrics = pm.get_metrics();
        assert!(metrics.drawdown_pct > 0.0);
    }

    #[test]
    fn test_net_position() {
        let pm = PortfolioManager::new(100_000.0, 0.001);
        pm.on_fill("AAPL", 150.0, 10.0, true);
        pm.on_fill("AAPL", 152.0, 5.0, true);
        assert_eq!(pm.net_position("AAPL"), 15.0);
    }
}
