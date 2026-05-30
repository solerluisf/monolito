use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A consistent point-in-time snapshot of portfolio state for risk evaluation.
/// Captured under a single read lock so all checks see the same state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskSnapshot {
    pub metrics: PortfolioMetrics,
    pub net_positions: HashMap<String, f64>,
}

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

        Self::recalculate_equity(&mut state);
    }

    pub fn update_price(&self, symbol: &str, price: f64) {
        let mut state = self.state.write();
        if let Some(pos) = state.positions.get_mut(symbol) {
            pos.current_price = price;
        }
        Self::recalculate_equity(&mut state);
    }

    fn metrics_from_state(state: &PortfolioState) -> PortfolioMetrics {
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

    fn recalculate_equity(state: &mut PortfolioState) {
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
        Self::metrics_from_state(&state)
    }

    /// Consistent point-in-time snapshot for risk evaluation.
    /// All fields are derived under a single read lock.
    pub fn get_risk_snapshot(&self) -> RiskSnapshot {
        let state = self.state.read();
        let metrics = Self::metrics_from_state(&state);
        let net_positions = state.positions.iter()
            .map(|(k, v)| (k.clone(), v.net_position))
            .collect();
        RiskSnapshot { metrics, net_positions }
    }

    /// Set the average entry price for a position (used during broker reconciliation).
    pub fn set_avg_entry_price(&self, symbol: &str, price: f64) {
        let mut state = self.state.write();
        if let Some(pos) = state.positions.get_mut(symbol) {
            pos.avg_entry_price = price;
        }
        Self::recalculate_equity(&mut state);
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

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

    #[test]
    fn test_concurrent_fills_and_snapshots_are_consistent() {
        let pm = Arc::new(PortfolioManager::new(1_000_000.0, 0.001));
        let symbols: Vec<String> = (0..8).map(|i| format!("SYM{}", i)).collect();
        let num_writers = 4;
        let ops_per_writer = 500;
        let running = Arc::new(AtomicBool::new(true));

        // Spawn writer threads that hammer on_fill and update_price
        let mut writers = Vec::new();
        for w in 0..num_writers {
            let pm = Arc::clone(&pm);
            let syms = symbols.clone();
            let run = Arc::clone(&running);
            writers.push(std::thread::spawn(move || {
                for i in 0..ops_per_writer {
                    let idx = (w * ops_per_writer + i) % syms.len();
                    let price = 100.0 + (i % 50) as f64;
                    let qty = 1.0 + (i % 10) as f64;
                    let is_buy = i % 2 == 0;
                    pm.on_fill(&syms[idx], price, qty, is_buy);
                    // Every 10 ops, also update price to generate equity churn
                    if i % 10 == 0 {
                        pm.update_price(&syms[(idx + 1) % syms.len()], price * 1.02);
                    }
                }
                run.store(false, Ordering::Relaxed);
            }));
        }

        // Reader: continuously capture RiskSnapshot and verify invariants
        let pm_reader = Arc::clone(&pm);
        let run_reader = Arc::clone(&running);
        let reader = std::thread::spawn(move || {
            let initial_eq = 1_000_000.0;
            loop {
                let snap = pm_reader.get_risk_snapshot();

                // Invariant 1: All values are finite
                assert!(snap.metrics.current_equity.is_finite());
                assert!(snap.metrics.gross_exposure.is_finite());
                assert!(snap.metrics.leverage.is_finite());
                assert!(snap.metrics.drawdown_pct.is_finite());
                assert!(snap.metrics.total_unrealized_pnl.is_finite());
                assert!(snap.metrics.total_realized_pnl.is_finite());

                // Invariant 2: Non-negative where expected
                assert!(snap.metrics.current_equity >= 0.0);
                assert!(snap.metrics.gross_exposure >= 0.0);
                assert!(snap.metrics.leverage >= 0.0);
                assert!(snap.metrics.drawdown_pct >= 0.0);

                // Invariant 3: net_positions map entries have finite values
                for (_, &net) in &snap.net_positions {
                    assert!(net.is_finite(), "non-finite net_position in snapshot");
                }

                // Invariant 4: current_equity = initial_equity + unrealized + realized
                let computed_equity = initial_eq
                    + snap.metrics.total_unrealized_pnl
                    + snap.metrics.total_realized_pnl;
                assert!((snap.metrics.current_equity - computed_equity).abs() < 0.01,
                    "equity mismatch: current={} vs computed={} (unrealized={}, realized={})",
                    snap.metrics.current_equity, computed_equity,
                    snap.metrics.total_unrealized_pnl, snap.metrics.total_realized_pnl);

                // Invariant 5: leverage = gross_exposure / current_equity (when equity > 0)
                if snap.metrics.current_equity > 0.0 && snap.metrics.leverage > 0.0 {
                    let expected_lev = snap.metrics.gross_exposure / snap.metrics.current_equity;
                    assert!((snap.metrics.leverage - expected_lev).abs() < 0.01,
                        "leverage mismatch: {} vs {}", snap.metrics.leverage, expected_lev);
                }

                if !run_reader.load(Ordering::Relaxed) {
                    // Writers done; do one final verification and exit
                    break;
                }
            }
        });

        for h in writers {
            h.join().expect("writer panicked");
        }
        reader.join().expect("reader panicked");
    }
}
