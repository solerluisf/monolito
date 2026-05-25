use std::time::{SystemTime, UNIX_EPOCH};

use crate::portfolio_manager::PortfolioManager;
use unified_trading_core::config::RiskConfig;

#[derive(Debug, Clone)]
pub struct RiskCheckRequest {
    pub request_id: String,
    pub symbol: String,
    pub intent_id: String,
    pub side: String,
    pub quantity: f64,
    pub price: f64,
    pub timestamp_ns: u64,
    pub current_volatility: f64,
    pub current_spread_bps: f64,
}

#[derive(Debug, Clone)]
pub struct RiskDecision {
    pub request_id: String,
    pub approved: bool,
    pub rejection_reason: Option<String>,
    pub check_index: usize,
    pub timestamp_ns: u64,
}

impl RiskDecision {
    pub fn approved(request_id: &str) -> Self {
        Self {
            request_id: request_id.to_string(),
            approved: true,
            rejection_reason: None,
            check_index: 14,
            timestamp_ns: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
        }
    }

    pub fn rejected(request_id: &str, reason: &str, check_index: usize) -> Self {
        Self {
            request_id: request_id.to_string(),
            approved: false,
            rejection_reason: Some(reason.to_string()),
            check_index,
            timestamp_ns: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
        }
    }
}

pub struct RiskEngine {
    pub config: RiskConfig,
    pub portfolio: PortfolioManager,
    pub idempotency_store: std::collections::HashSet<String>,
    pub order_rate_tokens: f64,
    pub order_rate_last_refill: u64,
}

impl RiskEngine {
    pub fn new(config: RiskConfig, initial_equity: f64) -> Self {
        let max_order_rate = config.max_order_rate_per_sec as f64;
        Self {
            config,
            portfolio: PortfolioManager::new(initial_equity),
            idempotency_store: std::collections::HashSet::new(),
            order_rate_tokens: max_order_rate,
            order_rate_last_refill: 0,
        }
    }

    #[tracing::instrument(skip_all, fields(symbol = %request.symbol, intent_id = %request.intent_id))]
    pub fn check(&mut self, request: &RiskCheckRequest, kill_switch_active: bool) -> RiskDecision {
        if let Some(decision) = Self::check_kill_switch(self, request, kill_switch_active) {
            return decision;
        }
        if let Some(decision) = Self::check_idempotency(self, request, kill_switch_active) {
            return decision;
        }
        if let Some(decision) = Self::check_staleness(self, request, kill_switch_active) {
            return decision;
        }
        if let Some(decision) = Self::check_order_rate(self, request, kill_switch_active) {
            return decision;
        }
        if let Some(decision) = Self::check_position_limit(self, request, kill_switch_active) {
            return decision;
        }
        if let Some(decision) = Self::check_portfolio_exposure(self, request, kill_switch_active) {
            return decision;
        }
        if let Some(decision) = Self::check_leverage(self, request, kill_switch_active) {
            return decision;
        }
        if let Some(decision) = Self::check_drawdown(self, request, kill_switch_active) {
            return decision;
        }
        if let Some(decision) = Self::check_volatility(self, request, kill_switch_active) {
            return decision;
        }
        if let Some(decision) = Self::check_spread(self, request, kill_switch_active) {
            return decision;
        }

        self.idempotency_store.insert(request.intent_id.clone());
        RiskDecision::approved(&request.request_id)
    }

    fn check_kill_switch(&mut self, _request: &RiskCheckRequest, kill_switch_active: bool) -> Option<RiskDecision> {
        if kill_switch_active {
            return Some(RiskDecision::rejected(
                &_request.request_id,
                "Kill switch active",
                0,
            ));
        }
        None
    }

    fn check_idempotency(&self, request: &RiskCheckRequest, _ks: bool) -> Option<RiskDecision> {
        if self.idempotency_store.contains(&request.intent_id) {
            return Some(RiskDecision::rejected(
                &request.request_id,
                "Duplicate intent_id",
                1,
            ));
        }
        None
    }

    fn check_staleness(&self, request: &RiskCheckRequest, _ks: bool) -> Option<RiskDecision> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let age_ns = now.saturating_sub(request.timestamp_ns);
        if age_ns > 2_000_000_000 {
            return Some(RiskDecision::rejected(
                &request.request_id,
                "Intent expired",
                2,
            ));
        }
        None
    }

    fn check_order_rate(&mut self, request: &RiskCheckRequest, _ks: bool) -> Option<RiskDecision> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        if self.order_rate_last_refill > 0 {
            let elapsed_s = (now - self.order_rate_last_refill) as f64 / 1_000_000_000.0;
            self.order_rate_tokens = (self.order_rate_tokens + elapsed_s * self.config.max_order_rate_per_sec as f64)
                .min(self.config.max_order_rate_per_sec as f64);
        }
        self.order_rate_last_refill = now;

        if self.order_rate_tokens < 1.0 {
            return Some(RiskDecision::rejected(
                &request.request_id,
                "Order rate limit exceeded",
                3,
            ));
        }
        self.order_rate_tokens -= 1.0;
        None
    }

    fn check_position_limit(&self, request: &RiskCheckRequest, _ks: bool) -> Option<RiskDecision> {
        let current = self.portfolio.net_position(&request.symbol);
        let new_position = current + request.quantity;
        if new_position.abs() * request.price > self.config.max_position_per_symbol {
            return Some(RiskDecision::rejected(
                &request.request_id,
                "Position limit exceeded",
                4,
            ));
        }
        None
    }

    fn check_portfolio_exposure(&self, request: &RiskCheckRequest, _ks: bool) -> Option<RiskDecision> {
        let metrics = self.portfolio.get_metrics();
        let new_exposure = metrics.gross_exposure + request.quantity * request.price;
        if new_exposure > self.config.max_portfolio_exposure {
            return Some(RiskDecision::rejected(
                &request.request_id,
                "Portfolio exposure limit exceeded",
                5,
            ));
        }
        None
    }

    fn check_leverage(&self, request: &RiskCheckRequest, _ks: bool) -> Option<RiskDecision> {
        let metrics = self.portfolio.get_metrics();
        let new_exposure = metrics.gross_exposure + request.quantity * request.price;
        let new_leverage = if metrics.current_equity > 0.0 {
            new_exposure / metrics.current_equity
        } else {
            0.0
        };
        if new_leverage > self.config.max_leverage {
            return Some(RiskDecision::rejected(
                &request.request_id,
                "Leverage limit exceeded",
                6,
            ));
        }
        None
    }

    fn check_drawdown(&self, request: &RiskCheckRequest, _ks: bool) -> Option<RiskDecision> {
        let metrics = self.portfolio.get_metrics();
        if metrics.drawdown_pct > self.config.max_drawdown_pct {
            return Some(RiskDecision::rejected(
                &request.request_id,
                "Drawdown limit exceeded",
                7,
            ));
        }
        None
    }

    fn check_volatility(&self, request: &RiskCheckRequest, _ks: bool) -> Option<RiskDecision> {
        if request.current_volatility > self.config.max_volatility {
            return Some(RiskDecision::rejected(
                &request.request_id,
                &format!("Volatility too high: {:.4} > {:.4}", request.current_volatility, self.config.max_volatility),
                8,
            ));
        }
        None
    }

    fn check_spread(&self, request: &RiskCheckRequest, _ks: bool) -> Option<RiskDecision> {
        if request.current_spread_bps > self.config.max_spread_bps {
            return Some(RiskDecision::rejected(
                &request.request_id,
                &format!("Spread too wide: {:.2} bps > {:.2} bps", request.current_spread_bps, self.config.max_spread_bps),
                9,
            ));
        }
        None
    }

    pub fn update_fill(&mut self, symbol: &str, price: f64, quantity: f64, is_buy: bool) {
        self.portfolio.update_position(symbol, price, quantity, is_buy);
    }

    pub fn update_market_price(&mut self, symbol: &str, price: f64) {
        self.portfolio.update_market_price(symbol, price);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn make_request(symbol: &str, quantity: f64, price: f64) -> RiskCheckRequest {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        RiskCheckRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            symbol: symbol.to_string(),
            intent_id: uuid::Uuid::new_v4().to_string(),
            side: "buy".to_string(),
            quantity,
            price,
            timestamp_ns: now,
            current_volatility: 0.01,
            current_spread_bps: 10.0,
        }
    }

    #[test]
    fn test_risk_engine_approve() {
        let config = RiskConfig::default();
        let mut engine = RiskEngine::new(config, 100_000.0);
        let request = make_request("AAPL", 10.0, 150.0);
        let decision = engine.check(&request, false);
        assert!(decision.approved);
    }

    #[test]
    fn test_risk_engine_kill_switch() {
        let config = RiskConfig::default();
        let mut engine = RiskEngine::new(config, 100_000.0);
        let request = make_request("AAPL", 10.0, 150.0);
        let decision = engine.check(&request, true);
        assert!(!decision.approved);
        assert!(decision.rejection_reason.unwrap().contains("Kill switch"));
    }

    #[test]
    fn test_risk_engine_idempotency() {
        let config = RiskConfig::default();
        let mut engine = RiskEngine::new(config, 100_000.0);
        let request = make_request("AAPL", 10.0, 150.0);
        let id = request.intent_id.clone();

        let decision1 = engine.check(&request, false);
        assert!(decision1.approved);

        let mut request2 = make_request("AAPL", 10.0, 150.0);
        request2.intent_id = id;
        let decision2 = engine.check(&request2, false);
        assert!(!decision2.approved);
    }

    #[test]
    fn test_risk_engine_position_limit() {
        let mut config = RiskConfig::default();
        config.max_position_per_symbol = 100.0;
        let mut engine = RiskEngine::new(config, 100_000.0);

        let request = make_request("AAPL", 100.0, 150.0);
        let decision = engine.check(&request, false);
        assert!(!decision.approved);
    }

    #[test]
    fn test_risk_engine_exposure_limit() {
        let mut config = RiskConfig::default();
        config.max_portfolio_exposure = 100.0;
        let mut engine = RiskEngine::new(config, 100_000.0);

        let request = make_request("AAPL", 10.0, 150.0);
        let decision = engine.check(&request, false);
        assert!(!decision.approved);
    }

    #[test]
    fn test_risk_engine_leverage_limit() {
        let mut config = RiskConfig::default();
        config.max_leverage = 0.001;
        let mut engine = RiskEngine::new(config, 100_000.0);

        let request = make_request("AAPL", 100.0, 150.0);
        let decision = engine.check(&request, false);
        assert!(!decision.approved);
    }

    #[test]
    fn test_risk_engine_volatility_limit() {
        let mut config = RiskConfig::default();
        config.max_volatility = 0.02;
        let mut engine = RiskEngine::new(config, 100_000.0);

        let mut request = make_request("AAPL", 10.0, 150.0);
        request.current_volatility = 0.05; // Above threshold
        
        let decision = engine.check(&request, false);
        assert!(!decision.approved);
        assert!(decision.rejection_reason.unwrap().contains("Volatility"));
    }

    #[test]
    fn test_risk_engine_spread_limit() {
        let mut config = RiskConfig::default();
        config.max_spread_bps = 20.0;
        let mut engine = RiskEngine::new(config, 100_000.0);

        let mut request = make_request("AAPL", 10.0, 150.0);
        request.current_spread_bps = 50.0; // Above threshold
        
        let decision = engine.check(&request, false);
        assert!(!decision.approved);
        assert!(decision.rejection_reason.unwrap().contains("Spread"));
    }

    #[test]
    fn test_risk_engine_volatility_passes() {
        let mut config = RiskConfig::default();
        config.max_volatility = 0.05;
        let mut engine = RiskEngine::new(config, 100_000.0);

        let mut request = make_request("AAPL", 10.0, 150.0);
        request.current_volatility = 0.01; // Below threshold
        
        let decision = engine.check(&request, false);
        assert!(decision.approved);
    }

    #[test]
    fn test_risk_engine_spread_passes() {
        let mut config = RiskConfig::default();
        config.max_spread_bps = 50.0;
        let mut engine = RiskEngine::new(config, 100_000.0);

        let mut request = make_request("AAPL", 10.0, 150.0);
        request.current_spread_bps = 10.0; // Below threshold
        
        let decision = engine.check(&request, false);
        assert!(decision.approved);
    }
}
