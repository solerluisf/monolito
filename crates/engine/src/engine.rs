use std::sync::atomic::Ordering;
use std::sync::Arc;

use crossbeam_channel::{bounded, Receiver, Sender};

use unified_trading_core::config::EngineConfig;
use unified_trading_core::metrics::GlobalMetrics;
use unified_trading_core::kill_switch::KillSwitch;
use unified_trading_core::journal::{JournalWriter, JournalEntry};
use unified_trading_core::heartbeat::ThreadHeartbeatMonitor;
use unified_trading_core::command_channel::{CommandChannel, CommandActor, ControlCommand, ControlResponse};
use unified_trading_core::threading::{spawn_pinned, ThreadPriority};
use unified_trading_core::position_manager::PositionManager;
use unified_trading_core::config_watcher::ConfigWatcher;
use parking_lot::RwLock;

use market_data::{Normalizer, RawTick};
use feature::{FeatureEngine, FeatureVector};
use model::{PredictionEngine, InferenceEngine};
use strategy::{StrategyEngine, TradeIntent};
use risk::{RiskCoordinator, RiskCheckRequest, RiskDecision};
use execution::{ExecutionManager, OrderLifecycleEvent};
use gateway::{AlpacaFeedConfig, AlpacaWebSocketFeed, AlpacaExecutionPort, MockExecutionPort, IExecutionPort};

use crate::tick_reactor::{spawn_reactor, ReactorCommand};

pub fn recv_batch<T>(rx: &Receiver<T>, buf: &mut Vec<T>, max: usize) -> usize {
    buf.clear();
    match rx.try_recv() {
        Ok(item) => buf.push(item),
        Err(_) => return 0,
    }
    for _ in 1..max {
        match rx.try_recv() {
            Ok(item) => buf.push(item),
            Err(_) => break,
        }
    }
    buf.len()
}

use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Mutex;

type StrategySwapRef = Arc<ArcSwap<Box<dyn strategy::Strategy>>>;

pub struct AssetProcessor {
    pub symbol: String,
    pub normalizer: Normalizer,
    pub feature_engine: FeatureEngine,
    pub strategy: StrategySwapRef,
    pub signal_ctx: strategy::SignalContext,
    pub coordinator_tx: Sender<RiskCheckRequest>,
    pub feature_tx: Sender<FeatureVector>,
    pub kill_switch: Arc<KillSwitch>,
    pub metrics: Arc<GlobalMetrics>,
}

impl AssetProcessor {
    pub fn run_loop(&mut self, md_rx: &Receiver<RawTick>) {
        let mut batch = Vec::with_capacity(32);

        while !self.kill_switch.is_active() {
            let _count = recv_batch(md_rx, &mut batch, 32);

            for tick in batch.drain(..) {
                let normalized = self.normalizer.process(tick.clone());
                let features = self.feature_engine.compute(&normalized);
                let _ = self.feature_tx.try_send(features.clone());

                self.signal_ctx.update_price(normalized.mid_price);
                if normalized.mid_price > 0.0 {
                    let spread = (normalized.ask - normalized.bid) / normalized.mid_price * 10000.0;
                    self.signal_ctx.update_spread(spread.max(0.0));
                }

                let prediction = model::Prediction::from_features(&features, &self.symbol);

                let strat = self.strategy.load_full();
                if let Some(signal) = strat.evaluate(&prediction, &self.signal_ctx) {
                    let request = self.build_risk_request(&signal);
                    match self.coordinator_tx.try_send(request) {
                        Ok(()) => {
                            self.metrics.intents_generated.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(crossbeam_channel::TrySendError::Full(_)) => {
                            self.metrics.dropped_intents.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                            self.kill_switch.activate();
                        }
                    }
                }

                self.metrics.ticks_processed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn swap_strategy(&self, new_strategy: Box<dyn strategy::Strategy>) {
        tracing::info!(
            symbol = %self.symbol,
            from = %self.strategy.load().name(),
            to = %new_strategy.name(),
            "Hot-reloading strategy"
        );
        self.strategy.store(Arc::new(new_strategy));
    }

    pub fn build_risk_request(&self, signal: &TradeIntent) -> RiskCheckRequest {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        RiskCheckRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            symbol: signal.symbol.clone(),
            intent_id: signal.intent_id.clone(),
            side: format!("{:?}", signal.side),
            quantity: 1.0,
            price: 150.0,
            timestamp_ns: now,
        }
    }
}

pub struct UnifiedEngine {
    pub config: Arc<RwLock<EngineConfig>>,
    pub kill_switch: Arc<KillSwitch>,
    pub metrics: Arc<GlobalMetrics>,
    pub command_channel: CommandChannel,
    pub journal: Option<JournalWriter>,
    pub heartbeat_monitor: Option<ThreadHeartbeatMonitor>,
    pub config_watcher: Option<ConfigWatcher>,
    pub strategy_registry: Arc<Mutex<HashMap<String, StrategySwapRef>>>,
    next_asset_core: usize,
}

impl UnifiedEngine {
    pub fn new(config: EngineConfig) -> Self {
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        let command_channel = CommandChannel::new(1000);
        let strategy_registry = Arc::new(Mutex::new(HashMap::new()));
        let config = Arc::new(RwLock::new(config));

        Self {
            config: Arc::clone(&config),
            kill_switch,
            metrics,
            command_channel,
            journal: None,
            heartbeat_monitor: None,
            config_watcher: None,
            strategy_registry,
            next_asset_core: 1,
        }
    }

    pub fn start(&mut self) {
        tracing::info!("Unified Trading Engine starting...");

        let config = self.config.read();
        let journal = JournalWriter::new(
            &config.journal_config.journal_dir,
            config.journal_config.flush_interval_ms,
            Arc::clone(&self.metrics),
        );
        drop(config);
        self.journal = Some(journal);

        let heartbeat_monitor = ThreadHeartbeatMonitor::new(
            Arc::clone(&self.kill_switch),
            Arc::clone(&self.metrics),
            2_000_000_000,
            500,
        );
        self.heartbeat_monitor = Some(heartbeat_monitor);

        let symbols: Vec<String> = self.config.read().asset_configs
            .iter()
            .filter(|c| c.enabled)
            .map(|c| c.symbol.clone())
            .collect();

        let feed_rx = self.start_alpaca_feed(&symbols);

        self.start_assets(feed_rx);
        self.start_config_watcher();
        self.start_command_actor();

        tracing::info!("Unified Trading Engine running");
    }

    fn start_alpaca_feed(&self, symbols: &[String]) -> Option<Receiver<RawTick>> {
        if symbols.is_empty() {
            return None;
        }

        let config = self.config.read();
        let broker = &config.broker_config;
        if broker.api_key.is_empty() || broker.api_secret.is_empty() {
            tracing::warn!("Alpaca API credentials not configured, skipping live feed");
            return None;
        }

        let (feed_tx, feed_rx) = bounded::<RawTick>(10_000);

        let feed_config = AlpacaFeedConfig {
            api_key: broker.api_key.clone(),
            api_secret: broker.api_secret.clone(),
            paper_trading: broker.paper_trading,
            symbols: symbols.to_vec(),
            subscribe_trades: true,
            subscribe_quotes: true,
            subscribe_bars: false,
        };

        let feed = AlpacaWebSocketFeed::new(feed_config, feed_tx);

        let ks = Arc::clone(&self.kill_switch);

        std::thread::Builder::new()
            .name("alpaca-feed".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create tokio runtime");

                rt.block_on(async {
                    let handle = feed.start();
                    while !ks.is_active() {
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                    feed.stop();
                    let _ = handle.await;
                    tracing::info!("Alpaca feed stopped");
                });
            })
            .expect("Failed to spawn Alpaca feed thread");

        tracing::info!("Alpaca WebSocket feed started for {:?}", symbols);
        Some(feed_rx)
    }

    fn start_assets(&mut self, feed_rx: Option<Receiver<RawTick>>) {
        let mut md_tx_list: Vec<(String, Sender<RawTick>)> = Vec::new();
        let position_manager = Arc::new(PositionManager::new());
        let (lifecycle_tx, lifecycle_rx) = bounded::<OrderLifecycleEvent>(1000);

        let broker = &self.config.read().broker_config;
        let execution_port: Arc<dyn IExecutionPort> = if !broker.api_key.is_empty() && !broker.api_secret.is_empty() {
            match AlpacaExecutionPort::new(&broker.api_key, &broker.api_secret, broker.paper_trading) {
                Ok(port) => {
                    tracing::info!("Alpaca execution port initialized (paper={})", broker.paper_trading);
                    Arc::new(port)
                }
                Err(e) => {
                    tracing::warn!("Failed to initialize Alpaca execution port: {}, using mock", e);
                    Arc::new(MockExecutionPort)
                }
            }
        } else {
            tracing::warn!("Alpaca API credentials not configured, using mock execution port");
            Arc::new(MockExecutionPort)
        };

        let pm_clone = Arc::clone(&position_manager);
        let ks = Arc::clone(&self.kill_switch);
        let metrics = Arc::clone(&self.metrics);
        std::thread::Builder::new()
            .name("lifecycle-handler".to_string())
            .spawn(move || {
                tracing::info!("Order lifecycle handler started");
                while !ks.is_active() {
                    match lifecycle_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                        Ok(event) => {
                            tracing::info!(
                                event_type = ?event.event_type,
                                symbol = %event.symbol,
                                execution_id = %event.execution_id,
                                "Order lifecycle event"
                            );
                            metrics.orders_lifecycle_events.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                    }
                }
                tracing::info!("Order lifecycle handler stopped");
            })
            .expect("Failed to spawn lifecycle handler");

        for asset_config in &self.config.read().asset_configs {
            if !asset_config.enabled {
                continue;
            }

            let core_id = self.next_asset_core;
            self.next_asset_core += 1;
            if self.next_asset_core > 3 {
                self.next_asset_core = 1;
            }

            let (md_tx, md_rx) = bounded::<RawTick>(10_000);
            let (feature_tx, feature_rx) = bounded::<FeatureVector>(1000);
            let (risk_tx, risk_rx) = bounded::<RiskCheckRequest>(1000);
            let (decision_tx, decision_rx) = bounded::<RiskDecision>(1000);
            let (lifecycle_tx_clone, _order_rx) = (lifecycle_tx.clone(), bounded::<String>(1000).1);

            md_tx_list.push((asset_config.symbol.clone(), md_tx));

            let ks = Arc::clone(&self.kill_switch);
            let metrics = Arc::clone(&self.metrics);
            let pm = Arc::clone(&position_manager);

            let config = self.config.read();
            let strategy = StrategyEngine::new(
                &asset_config.symbol,
                config.strategy_config.long_entry_threshold,
                config.strategy_config.short_entry_threshold,
                config.strategy_config.confidence_minimum,
                config.strategy_config.hysteresis_deadband,
                config.strategy_config.entry_cooldown_ms,
                config.strategy_config.exit_cooldown_ms,
                config.strategy_config.prediction_staleness_ns,
                config.risk_config.allow_short,
            );

            let strategy_arc = Arc::new(ArcSwap::new(Arc::new(Box::new(strategy) as Box<dyn strategy::Strategy>)));

            self.strategy_registry.lock().unwrap().insert(
                asset_config.symbol.clone(),
                Arc::clone(&strategy_arc),
            );

            let signal_ctx = strategy::SignalContext::new(&asset_config.symbol);

            let processor = AssetProcessor {
                symbol: asset_config.symbol.clone(),
                normalizer: Normalizer::new(&asset_config.symbol),
                feature_engine: FeatureEngine::new(
                    &asset_config.symbol,
                    config.feature_config.rsi_period,
                    config.feature_config.atr_period,
                    20,
                ),
                strategy: strategy_arc,
                signal_ctx,
                coordinator_tx: risk_tx,
                feature_tx: feature_tx.clone(),
                kill_switch: Arc::clone(&ks),
                metrics: Arc::clone(&metrics),
            };

            let pred_engine = PredictionEngine::new(feature_rx, &asset_config.symbol);
            let inference_engine = InferenceEngine::new(config.model_config.feature_vector_size);

            let _pred_handle = pred_engine.start(move |features| {
                inference_engine.predict(features)
            });

            let risk_coordinator = RiskCoordinator::new(
                risk_rx,
                decision_tx,
                config.risk_config.clone(),
                100_000.0,
                Arc::clone(&ks),
                Arc::clone(&metrics),
            );
            let _risk_handle = risk_coordinator.start();

            let exec_manager = ExecutionManager::new(
                decision_rx,
                lifecycle_tx_clone,
                Arc::clone(&execution_port),
                config.risk_config.max_order_rate_per_sec as f64,
                config.risk_config.max_order_rate_per_sec as f64 / 2.0,
                Arc::clone(&metrics),
                Arc::clone(&ks),
                pm,
            );
            drop(config);
            let _exec_handle = exec_manager.start();

            let sym = asset_config.symbol.clone();
            spawn_pinned(
                &format!("asset-{}", sym),
                core_id,
                ThreadPriority::High,
                move || {
                    let mut processor = processor;
                    processor.run_loop(&md_rx);
                    tracing::info!("Asset processor for {} stopped", sym);
                },
            );

            tracing::info!("Started asset processor for {} on core {}", asset_config.symbol, core_id);
        }

        if let Some(feed_rx) = feed_rx {
            let (reactor_tx, reactor_handle) = spawn_reactor(
                feed_rx,
                Arc::clone(&self.kill_switch),
                Arc::clone(&self.metrics),
            );

            for (symbol, tx) in &md_tx_list {
                let _ = reactor_tx.send(ReactorCommand::Subscribe { symbol: symbol.clone(), tx: tx.clone() });
            }

            tracing::info!("Tick reactor started with {} subscriptions", md_tx_list.len());
        }
    }

    fn start_config_watcher(&mut self) {
        let config_path = std::env::var("TRADING_CONFIG").unwrap_or_else(|_| "config.toml".to_string());
        let mut watcher = ConfigWatcher::new(Arc::clone(&self.config));
        if let Err(e) = watcher.start(&config_path) {
            tracing::warn!("Failed to start config watcher: {}", e);
        } else {
            tracing::info!("Config watcher started for {}", config_path);
        }
        self.config_watcher = Some(watcher);
    }

    fn start_command_actor(&self) {
        let metrics = Arc::clone(&self.metrics);
        let kill_switch = Arc::clone(&self.kill_switch);
        let strategy_registry = Arc::clone(&self.strategy_registry);

        let _actor = CommandActor::new(self.command_channel.rx.clone(), move |cmd| {
            match cmd {
                ControlCommand::SetKillSwitch(active) => {
                    if active {
                        kill_switch.activate();
                    } else {
                        kill_switch.clear();
                    }
                    ControlResponse::Ok
                }
                ControlCommand::GetStatus => {
                    let snap = metrics.snapshot();
                    ControlResponse::Status(format!("{:?}", snap))
                }
                ControlCommand::Shutdown => {
                    kill_switch.activate();
                    ControlResponse::Ok
                }
                ControlCommand::SwapStrategy { symbol, strategy_type } => {
                    let registry = strategy_registry.lock().unwrap();
                    if let Some(strategy_ref) = registry.get(&symbol) {
                        let old_name = strategy_ref.load().name().to_string();
                        let new_strategy: Box<dyn strategy::Strategy> = match strategy_type.as_str() {
                            "hysteresis" => {
                                Box::new(StrategyEngine::new(&symbol, 0.6, -0.6, 0.5, 0.15, 5000, 2000, 150_000_000, true))
                            }
                            "conservative" => {
                                Box::new(StrategyEngine::new(&symbol, 0.8, -0.8, 0.6, 0.2, 10000, 5000, 200_000_000, false))
                            }
                            "aggressive" => {
                                Box::new(StrategyEngine::new(&symbol, 0.4, -0.4, 0.3, 0.1, 2000, 1000, 100_000_000, true))
                            }
                            _ => {
                                return ControlResponse::Error(format!("Unknown strategy type: {}", strategy_type));
                            }
                        };
                        strategy_ref.store(Arc::new(new_strategy));
                        tracing::info!(symbol = %symbol, from = %old_name, to = %strategy_type, "Strategy swapped");
                        ControlResponse::Ok
                    } else {
                        ControlResponse::Error(format!("Symbol not found: {}", symbol))
                    }
                }
                _ => ControlResponse::Ok,
            }
        });
    }

    pub fn shutdown(&mut self) {
        self.kill_switch.activate();
        tracing::info!("Unified Trading Engine shutting down...");

        if let Some(journal) = self.journal.take() {
            journal.shutdown();
        }

        if let Some(mut monitor) = self.heartbeat_monitor.take() {
            monitor.shutdown();
        }

        if let Some(mut watcher) = self.config_watcher.take() {
            watcher.stop();
        }

        let snap = self.metrics.snapshot();
        tracing::info!("Final metrics: {:?}", snap);
    }
}
