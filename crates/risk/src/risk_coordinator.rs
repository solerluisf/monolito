use crossbeam_channel::{bounded, Receiver, Sender};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use unified_trading_core::kill_switch::KillSwitch;
use unified_trading_core::config::RiskConfig;
use unified_trading_core::metrics::GlobalMetrics;
use unified_trading_core::threading::{spawn_pinned, ThreadPriority};
use unified_trading_core::portfolio_manager::PortfolioManager;

use crate::risk_checks::{RiskCheckRequest, RiskDecision, RiskEngine};

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
        portfolio: Arc<PortfolioManager>,
        kill_switch: Arc<KillSwitch>,
        metrics: Arc<GlobalMetrics>,
    ) -> Self {
        let severity_overrides = config.severity_overrides.clone();
        let mut engine = RiskEngine::new(config, portfolio);
        engine.severity_overrides = severity_overrides;
        Self {
            request_rx,
            decision_tx,
            engine,
            kill_switch,
            metrics,
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn run_loop(&mut self) {
        while self.running.load(Ordering::Relaxed) {
            match self.request_rx.recv_timeout(std::time::Duration::from_millis(10)) {
                Ok(request) => {
                    let check_start = std::time::Instant::now();
                    self.metrics.risk_channel_depth.fetch_sub(1, Ordering::Relaxed);
                    let ks_active = self.kill_switch.is_active();
                    let decision = self.engine.check(&request, ks_active);

                    if decision.approved {
                        self.metrics.intents_approved.fetch_add(1, Ordering::Relaxed);
                        self.metrics.increment_per_symbol_intent_approved("UNK");
                    } else {
                        self.metrics.intents_rejected.fetch_add(1, Ordering::Relaxed);
                        self.metrics.increment_per_symbol_intent_rejected("UNK");
                    }

                    if self.decision_tx.send(decision).is_err() {
                        break;
                    }
                    self.metrics.decision_channel_depth.fetch_add(1, Ordering::Relaxed);
                    let elapsed_ns = check_start.elapsed().as_nanos() as u64;
                    self.metrics.risk_check_latency.record(elapsed_ns);
                    let decision_latency_ns = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as u64;
                    let total_latency = decision_latency_ns.saturating_sub(request.timestamp_ns);
                    self.metrics.decision_latency.record(total_latency);
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }
        }

        // Drain remaining risk requests so they are not lost in the queue
        while let Ok(request) = self.request_rx.try_recv() {
            self.metrics.risk_channel_depth.fetch_sub(1, Ordering::Relaxed);
            let decision = self.engine.check(&request, true); // reject under shutdown
            let _ = self.decision_tx.try_send(decision);
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
        ).expect("spawn_pinned failed")
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

use unified_trading_core::symbol_registry::SymbolId;

    fn make_request(symbol_id: SymbolId) -> RiskCheckRequest {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        RiskCheckRequest {
            request_id: unified_trading_core::symbol_registry::next_request_id(),
            symbol_id,
            intent_id: unified_trading_core::symbol_registry::next_intent_id(),
            side: 1u8, // Buy
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
            Arc::new(PortfolioManager::new(100_000.0, 0.001)),
            Arc::clone(&kill_switch),
            Arc::clone(&metrics),
        );

        req_tx.send(make_request(SymbolId::from_raw(0))).unwrap();
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
            Arc::new(PortfolioManager::new(100_000.0, 0.001)),
            Arc::clone(&kill_switch),
            Arc::clone(&metrics),
        );

        req_tx.send(make_request(SymbolId::from_raw(0))).unwrap();
        drop(req_tx);

        let handle = coordinator.start(0);
        let decision = dec_rx.recv_timeout(std::time::Duration::from_millis(200)).unwrap();
        assert!(!decision.approved);
        let _ = handle.join();
    }
}
