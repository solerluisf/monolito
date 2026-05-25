use crossbeam_channel::{bounded, Receiver, Sender};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use unified_trading_core::kill_switch::KillSwitch;
use unified_trading_core::config::RiskConfig;
use unified_trading_core::metrics::GlobalMetrics;
use unified_trading_core::threading::{spawn_pinned, ThreadPriority};

use crate::risk_checks::{RiskCheckRequest, RiskDecision, RiskEngine};
use crate::portfolio_manager::PortfolioManager;

pub struct RiskCoordinator {
    pub request_rx: Receiver<RiskCheckRequest>,
    pub decision_tx: Sender<RiskDecision>,
    pub engine: RiskEngine,
    pub kill_switch: Arc<KillSwitch>,
    pub metrics: Arc<GlobalMetrics>,
    running: Arc<AtomicBool>,
}

impl RiskCoordinator {
    pub fn new(
        request_rx: Receiver<RiskCheckRequest>,
        decision_tx: Sender<RiskDecision>,
        config: RiskConfig,
        initial_equity: f64,
        kill_switch: Arc<KillSwitch>,
        metrics: Arc<GlobalMetrics>,
    ) -> Self {
        Self {
            request_rx,
            decision_tx,
            engine: RiskEngine::new(config, initial_equity),
            kill_switch,
            metrics,
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn run_loop(&mut self) {
        while self.running.load(Ordering::Relaxed) {
            match self.request_rx.recv_timeout(std::time::Duration::from_millis(10)) {
                Ok(request) => {
                    let ks_active = self.kill_switch.is_active();
                    let decision = self.engine.check(&request, ks_active);

                    if decision.approved {
                        self.metrics.intents_approved.fetch_add(1, Ordering::Relaxed);
                    } else {
                        self.metrics.intents_rejected.fetch_add(1, Ordering::Relaxed);
                    }

                    if self.decision_tx.send(decision).is_err() {
                        break;
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    pub fn start(mut self, core_id: usize) -> std::thread::JoinHandle<()> {
        spawn_pinned(
            "risk-coordinator",
            core_id,
            ThreadPriority::High,
            move || {
                self.run_loop();
            },
        )
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    pub fn update_fill(&mut self, symbol: &str, price: f64, quantity: f64, is_buy: bool) {
        self.engine.update_fill(symbol, price, quantity, is_buy);
    }

    pub fn update_market_price(&mut self, symbol: &str, price: f64) {
        self.engine.update_market_price(symbol, price);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn make_request(symbol: &str) -> RiskCheckRequest {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        RiskCheckRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            symbol: symbol.to_string(),
            intent_id: uuid::Uuid::new_v4().to_string(),
            side: "buy".to_string(),
            quantity: 10.0,
            price: 150.0,
            timestamp_ns: now,
            current_volatility: 0.01,
            current_spread_bps: 10.0,
        }
    }

    #[test]
    fn test_risk_coordinator_approves_valid_request() {
        let (req_tx, req_rx) = bounded::<RiskCheckRequest>(100);
        let (dec_tx, dec_rx) = bounded::<RiskDecision>(100);
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());

        let coordinator = RiskCoordinator::new(
            req_rx,
            dec_tx,
            RiskConfig::default(),
            100_000.0,
            Arc::clone(&kill_switch),
            Arc::clone(&metrics),
        );

        req_tx.send(make_request("AAPL")).unwrap();
        drop(req_tx);

        let handle = coordinator.start(0);
        let decision = dec_rx.recv_timeout(std::time::Duration::from_millis(200)).unwrap();
        assert!(decision.approved);
        let _ = handle.join();
    }

    #[test]
    fn test_risk_coordinator_rejects_on_killswitch() {
        let (req_tx, req_rx) = bounded::<RiskCheckRequest>(100);
        let (dec_tx, dec_rx) = bounded::<RiskDecision>(100);
        let kill_switch = Arc::new(KillSwitch::new());
        kill_switch.activate();
        let metrics = Arc::new(GlobalMetrics::new());

        let coordinator = RiskCoordinator::new(
            req_rx,
            dec_tx,
            RiskConfig::default(),
            100_000.0,
            Arc::clone(&kill_switch),
            Arc::clone(&metrics),
        );

        req_tx.send(make_request("AAPL")).unwrap();
        drop(req_tx);

        let handle = coordinator.start(0);
        let decision = dec_rx.recv_timeout(std::time::Duration::from_millis(200)).unwrap();
        assert!(!decision.approved);
        let _ = handle.join();
    }
}
