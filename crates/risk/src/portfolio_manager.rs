use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionState {
    pub symbol: String,
    pub net_position: f64,
    pub avg_entry_price: f64,
    pub current_price: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
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
    pub positions: HashMap<String, PositionState>,
    pub open_orders: HashMap<String, OpenOrder>,
    pub peak_equity: f64,
    pub current_equity: f64,
    pub initial_equity: f64,
}

impl PortfolioManager {
    pub fn new(initial_equity: f64) -> Self {
        Self {
            positions: HashMap::new(),
            open_orders: HashMap::new(),
            peak_equity: initial_equity,
            current_equity: initial_equity,
            initial_equity,
        }
    }

    pub fn update_position(&mut self, symbol: &str, fill_price: f64, quantity: f64, is_buy: bool) {
        let pos = self.positions.entry(symbol.to_string()).or_insert_with(|| PositionState {
            symbol: symbol.to_string(),
            net_position: 0.0,
            avg_entry_price: 0.0,
            current_price: fill_price,
            unrealized_pnl: 0.0,
            realized_pnl: 0.0,
        });

        let delta = if is_buy { quantity } else { -quantity };

        if (pos.net_position >= 0.0 && delta > 0.0) || (pos.net_position <= 0.0 && delta < 0.0) {
            let total_qty = pos.net_position.abs() + delta.abs();
            pos.avg_entry_price = (pos.avg_entry_price * pos.net_position.abs() + fill_price * delta.abs()) / total_qty;
        } else {
            let closed_qty = delta.abs().min(pos.net_position.abs());
            let pnl = closed_qty * (fill_price - pos.avg_entry_price) * pos.net_position.signum();
            pos.realized_pnl += pnl;
        }

        pos.net_position += delta;
        pos.current_price = fill_price;

        if pos.net_position.abs() < 0.001 {
            pos.avg_entry_price = 0.0;
        }

        self.recalculate_equity();
    }

    pub fn update_market_price(&mut self, symbol: &str, price: f64) {
        if let Some(pos) = self.positions.get_mut(symbol) {
            pos.current_price = price;
        }
        self.recalculate_equity();
    }

    pub fn recalculate_equity(&mut self) {
        let unrealized: f64 = self.positions.values().map(|p| {
            p.net_position * (p.current_price - p.avg_entry_price)
        }).sum();

        let realized: f64 = self.positions.values().map(|p| p.realized_pnl).sum();

        self.current_equity = self.initial_equity + unrealized + realized;

        if self.current_equity > self.peak_equity {
            self.peak_equity = self.current_equity;
        }
    }

    pub fn get_metrics(&self) -> PortfolioMetrics {
        let gross: f64 = self.positions.values().map(|p| p.net_position.abs() * p.current_price).sum();
        let net: f64 = self.positions.values().map(|p| p.net_position * p.current_price).sum();
        let unrealized: f64 = self.positions.values().map(|p| p.unrealized_pnl).sum();
        let realized: f64 = self.positions.values().map(|p| p.realized_pnl).sum();

        let drawdown = if self.peak_equity > 0.0 {
            (self.peak_equity - self.current_equity) / self.peak_equity * 100.0
        } else {
            0.0
        };

        let leverage = if self.current_equity > 0.0 {
            gross / self.current_equity
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
            peak_equity: self.peak_equity,
            current_equity: self.current_equity,
            drawdown_pct: drawdown,
        }
    }

    pub fn get_position(&self, symbol: &str) -> Option<&PositionState> {
        self.positions.get(symbol)
    }

    pub fn net_position(&self, symbol: &str) -> f64 {
        self.positions.get(symbol).map(|p| p.net_position).unwrap_or(0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_portfolio_manager_initial() {
        let pm = PortfolioManager::new(100_000.0);
        assert_eq!(pm.current_equity, 100_000.0);
        assert_eq!(pm.peak_equity, 100_000.0);
    }

    #[test]
    fn test_portfolio_manager_buy_position() {
        let mut pm = PortfolioManager::new(100_000.0);
        pm.update_position("AAPL", 150.0, 10.0, true);
        let pos = pm.get_position("AAPL").unwrap();
        assert_eq!(pos.net_position, 10.0);
        assert!((pos.avg_entry_price - 150.0).abs() < 0.01);
    }

    #[test]
    fn test_portfolio_manager_sell_position() {
        let mut pm = PortfolioManager::new(100_000.0);
        pm.update_position("AAPL", 150.0, 10.0, true);
        pm.update_position("AAPL", 155.0, 10.0, false);
        let pos = pm.get_position("AAPL").unwrap();
        assert!(pos.net_position.abs() < 0.01);
        assert!(pos.realized_pnl > 0.0);
    }

    #[test]
    fn test_portfolio_manager_drawdown() {
        let mut pm = PortfolioManager::new(100_000.0);
        pm.update_position("AAPL", 150.0, 100.0, true);
        pm.update_market_price("AAPL", 140.0);
        let metrics = pm.get_metrics();
        assert!(metrics.drawdown_pct > 0.0);
    }

    #[test]
    fn test_portfolio_manager_metrics() {
        let mut pm = PortfolioManager::new(100_000.0);
        pm.update_position("AAPL", 150.0, 10.0, true);
        let metrics = pm.get_metrics();
        assert!(metrics.gross_exposure > 0.0);
        assert!(metrics.leverage >= 0.0);
    }

    #[test]
    fn test_portfolio_manager_net_position() {
        let mut pm = PortfolioManager::new(100_000.0);
        pm.update_position("AAPL", 150.0, 10.0, true);
        pm.update_position("AAPL", 152.0, 5.0, true);
        assert_eq!(pm.net_position("AAPL"), 15.0);
    }
}
