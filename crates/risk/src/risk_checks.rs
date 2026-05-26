use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use unified_trading_core::config::{RiskConfig, CheckSeverity, default_check_severities};
use unified_trading_core::portfolio_manager::PortfolioManager;

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
    pub warnings: Vec<String>,
    pub check_index: usize,
    pub timestamp_ns: u64,
    pub request: RiskCheckRequest,
}

impl RiskDecision {
    pub fn approved(request: &RiskCheckRequest) -> Self {
        Self {
            request_id: request.request_id.clone(),
            approved: true,
            rejection_reason: None,
            warnings: Vec::new(),
            check_index: 14,
            timestamp_ns: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
            request: request.clone(),
        }
    }

    pub fn rejected(request: &RiskCheckRequest, reason: &str, check_index: usize) -> Self {
        Self {
            request_id: request.request_id.clone(),
            approved: false,
            rejection_reason: Some(reason.to_string()),
            warnings: Vec::new(),
            check_index,
            timestamp_ns: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
            request: request.clone(),
        }
    }
}

pub struct RiskEngine {
    pub config: RiskConfig,
    pub portfolio: Arc<PortfolioManager>,
    pub idempotency_store: std::collections::HashSet<String>,
    pub order_rate_tokens: f64,
    pub order_rate_last_refill: u64,
    pub severity_overrides: HashMap<String, CheckSeverity>,
}

impl RiskEngine {
    pub fn new(config: RiskConfig, portfolio: Arc<PortfolioManager>) -> Self {
        let max_order_rate = config.max_order_rate_per_sec as f64;
        Self {
            config,
            portfolio,
            idempotency_store: std::collections::HashSet::new(),
            order_rate_tokens: max_order_rate,
            order_rate_last_refill: 0,
            severity_overrides: default_check_severities(),
        }
    }

    fn severity(&self, check_name: &str) -> CheckSeverity {
        self.severity_overrides
            .get(check_name)
            .copied()
            .unwrap_or(CheckSeverity::Veto)
    }

    #[tracing::instrument(skip_all, fields(symbol = %request.symbol, intent_id = %request.intent_id))]
    pub fn check(&mut self, request: &RiskCheckRequest, kill_switch_active: bool) -> RiskDecision {
        let mut warnings: Vec<String> = Vec::new();

        // Phase 1: Run all hard Veto checks first (these block the order)
        macro_rules! run_veto {
            ($name:literal, $index:expr, $check:expr) => {
                if let Some(reason) = $check {
                    match self.severity($name) {
                        CheckSeverity::Veto => {
                            return RiskDecision::rejected(request, &reason, $index);
                        }
                        CheckSeverity::Advisory => {
                            tracing::warn!(check = $name, reason = %reason, "Advisory risk check triggered");
                            warnings.push(reason);
                        }
                        CheckSeverity::Info => {
                            tracing::info!(check = $name, reason = %reason, "Info risk check note");
                        }
                    }
                }
            };
        }

        run_veto!("kill_switch", 0, self.check_kill_switch(kill_switch_active));
        run_veto!("idempotency", 1, self.check_idempotency(request));
        run_veto!("staleness", 2, self.check_staleness(request));
        run_veto!("order_rate", 3, self.check_order_rate(request));
        run_veto!("position_limit", 4, self.check_position_limit(request));
        run_veto!("portfolio_exposure", 5, self.check_portfolio_exposure(request));
        run_veto!("leverage", 6, self.check_leverage(request));
        run_veto!("drawdown", 7, self.check_drawdown(request));

        // Phase 2: Run Advisory / Info checks (volatility, spread)
        if let Some(reason) = self.check_volatility(request) {
            match self.severity("volatility") {
                CheckSeverity::Veto => return RiskDecision::rejected(request, &reason, 8),
                CheckSeverity::Advisory => {
                    tracing::warn!(check = "volatility", reason = %reason, "Advisory risk check triggered");
                    warnings.push(reason);
                }
                CheckSeverity::Info => {
                    tracing::info!(check = "volatility", reason = %reason, "Info risk check note");
                }
            }
        }

        if let Some(reason) = self.check_spread(request) {
            match self.severity("spread") {
                CheckSeverity::Veto => return RiskDecision::rejected(request, &reason, 9),
                CheckSeverity::Advisory => {
                    tracing::warn!(check = "spread", reason = %reason, "Advisory risk check triggered");
                    warnings.push(reason);
                }
                CheckSeverity::Info => {
                    tracing::info!(check = "spread", reason = %reason, "Info risk check note");
                }
            }
        }

        self.idempotency_store.insert(request.intent_id.clone());
        if warnings.is_empty() {
            RiskDecision::approved(request)
        } else {
            let mut decision = RiskDecision::approved(request);
            decision.warnings = warnings;
            decision
        }
    }

    fn check_kill_switch(&self, kill_switch_active: bool) -> Option<String> {
        if kill_switch_active {
            Some("Kill switch active".to_string())
        } else {
            None
        }
    }

    fn check_idempotency(&self, request: &RiskCheckRequest) -> Option<String> {
        if self.idempotency_store.contains(&request.intent_id) {
            Some("Duplicate intent_id".to_string())
        } else {
            None
        }
    }

    fn check_staleness(&self, request: &RiskCheckRequest) -> Option<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let age_ns = now.saturating_sub(request.timestamp_ns);
        if age_ns > self.config.risk_intent_staleness_ns {
            Some("Intent expired".to_string())
        } else {
            None
        }
    }

    fn check_order_rate(&mut self, _request: &RiskCheckRequest) -> Option<String> {
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
            Some("Order rate limit exceeded".to_string())
        } else {
            self.order_rate_tokens -= 1.0;
            None
        }
    }

    fn check_position_limit(&self, request: &RiskCheckRequest) -> Option<String> {
        let current = self.portfolio.net_position(&request.symbol);
        let new_position = current + request.quantity;
        if new_position.abs() * request.price > self.config.max_position_per_symbol {
            Some("Position limit exceeded".to_string())
        } else {
            None
        }
    }

    fn check_portfolio_exposure(&self, request: &RiskCheckRequest) -> Option<String> {
        let metrics = self.portfolio.get_metrics();
        let new_exposure = metrics.gross_exposure + request.quantity * request.price;
        if new_exposure > self.config.max_portfolio_exposure {
            Some("Portfolio exposure limit exceeded".to_string())
        } else {
            None
        }
    }

    fn check_leverage(&self, request: &RiskCheckRequest) -> Option<String> {
        let metrics = self.portfolio.get_metrics();
        let new_exposure = metrics.gross_exposure + request.quantity * request.price;
        let new_leverage = if metrics.current_equity > 0.0 {
            new_exposure / metrics.current_equity
        } else {
            0.0
        };
        if new_leverage > self.config.max_leverage {
            Some("Leverage limit exceeded".to_string())
        } else {
            None
        }
    }

    fn check_drawdown(&self, _request: &RiskCheckRequest) -> Option<String> {
        let metrics = self.portfolio.get_metrics();
        if metrics.drawdown_pct > self.config.max_drawdown_pct {
            Some("Drawdown limit exceeded".to_string())
        } else {
            None
        }
    }

    fn check_volatility(&self, request: &RiskCheckRequest) -> Option<String> {
        if request.current_volatility > self.config.max_volatility {
            Some(format!(
                "Volatility too high: {:.4} > {:.4}",
                request.current_volatility, self.config.max_volatility
            ))
        } else {
            None
        }
    }

    fn check_spread(&self, request: &RiskCheckRequest) -> Option<String> {
        if request.current_spread_bps > self.config.max_spread_bps {
            Some(format!(
                "Spread too wide: {:.2} bps > {:.2} bps",
                request.current_spread_bps, self.config.max_spread_bps
            ))
        } else {
            None
        }
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
        let mut engine = RiskEngine::new(config, Arc::new(PortfolioManager::new(100_000.0, 0.001)));
        let request = make_request("AAPL", 10.0, 150.0);
        let decision = engine.check(&request, false);
        assert!(decision.approved);
    }

    #[test]
    fn test_risk_engine_kill_switch() {
        let config = RiskConfig::default();
        let mut engine = RiskEngine::new(config, Arc::new(PortfolioManager::new(100_000.0, 0.001)));
        let request = make_request("AAPL", 10.0, 150.0);
        let decision = engine.check(&request, true);
        assert!(!decision.approved);
        assert!(decision.rejection_reason.unwrap().contains("Kill switch"));
    }

    #[test]
    fn test_risk_engine_idempotency() {
        let config = RiskConfig::default();
        let mut engine = RiskEngine::new(config, Arc::new(PortfolioManager::new(100_000.0, 0.001)));
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
        let mut engine = RiskEngine::new(config, Arc::new(PortfolioManager::new(100_000.0, 0.001)));

        let request = make_request("AAPL", 100.0, 150.0);
        let decision = engine.check(&request, false);
        assert!(!decision.approved);
    }

    #[test]
    fn test_risk_engine_exposure_limit() {
        let mut config = RiskConfig::default();
        config.max_portfolio_exposure = 100.0;
        let mut engine = RiskEngine::new(config, Arc::new(PortfolioManager::new(100_000.0, 0.001)));

        let request = make_request("AAPL", 10.0, 150.0);
        let decision = engine.check(&request, false);
        assert!(!decision.approved);
    }

    #[test]
    fn test_risk_engine_leverage_limit() {
        let mut config = RiskConfig::default();
        config.max_leverage = 0.001;
        let mut engine = RiskEngine::new(config, Arc::new(PortfolioManager::new(100_000.0, 0.001)));

        let request = make_request("AAPL", 100.0, 150.0);
        let decision = engine.check(&request, false);
        assert!(!decision.approved);
    }

    #[test]
    fn test_risk_engine_volatility_limit() {
        let mut config = RiskConfig::default();
        config.max_volatility = 0.02;
        let mut engine = RiskEngine::new(config, Arc::new(PortfolioManager::new(100_000.0, 0.001)));

        let mut request = make_request("AAPL", 10.0, 150.0);
        request.current_volatility = 0.05; // Above threshold
        
        let decision = engine.check(&request, false);
        // By default volatility is Advisory, so it should be approved with a warning
        assert!(decision.approved);
        assert!(decision.warnings.iter().any(|w| w.contains("Volatility")));
    }

    #[test]
    fn test_risk_engine_spread_limit() {
        let mut config = RiskConfig::default();
        config.max_spread_bps = 20.0;
        let mut engine = RiskEngine::new(config, Arc::new(PortfolioManager::new(100_000.0, 0.001)));

        let mut request = make_request("AAPL", 10.0, 150.0);
        request.current_spread_bps = 50.0; // Above threshold
        
        let decision = engine.check(&request, false);
        // By default spread is Advisory, so it should be approved with a warning
        assert!(decision.approved);
        assert!(decision.warnings.iter().any(|w| w.contains("Spread")));
    }

    #[test]
    fn test_risk_engine_volatility_veto() {
        let mut config = RiskConfig::default();
        config.max_volatility = 0.02;
        let mut engine = RiskEngine::new(config, Arc::new(PortfolioManager::new(100_000.0, 0.001)));
        engine.severity_overrides.insert("volatility".to_string(), CheckSeverity::Veto);

        let mut request = make_request("AAPL", 10.0, 150.0);
        request.current_volatility = 0.05; // Above threshold
        
        let decision = engine.check(&request, false);
        assert!(!decision.approved);
        assert!(decision.rejection_reason.unwrap().contains("Volatility"));
    }

    #[test]
    fn test_risk_engine_spread_veto() {
        let mut config = RiskConfig::default();
        config.max_spread_bps = 20.0;
        let mut engine = RiskEngine::new(config, Arc::new(PortfolioManager::new(100_000.0, 0.001)));
        engine.severity_overrides.insert("spread".to_string(), CheckSeverity::Veto);

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
        let mut engine = RiskEngine::new(config, Arc::new(PortfolioManager::new(100_000.0, 0.001)));

        let mut request = make_request("AAPL", 10.0, 150.0);
        request.current_volatility = 0.01; // Below threshold
        
        let decision = engine.check(&request, false);
        assert!(decision.approved);
        assert!(decision.warnings.is_empty());
    }

    #[test]
    fn test_risk_engine_spread_passes() {
        let mut config = RiskConfig::default();
        config.max_spread_bps = 50.0;
        let mut engine = RiskEngine::new(config, Arc::new(PortfolioManager::new(100_000.0, 0.001)));

        let mut request = make_request("AAPL", 10.0, 150.0);
        request.current_spread_bps = 10.0; // Below threshold
        
        let decision = engine.check(&request, false);
        assert!(decision.approved);
        assert!(decision.warnings.is_empty());
    }
}
