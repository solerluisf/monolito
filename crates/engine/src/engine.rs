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

use model::Prediction;

type StrategySwapRef = Arc<ArcSwap<Box<dyn strategy::Strategy>>>;
type PredictionRef = Arc<ArcSwap<Prediction>>;

pub struct AssetProcessor {
    pub symbol: String,
    pub normalizer: Normalizer,
    pub feature_engine: FeatureEngine,
    pub strategy: StrategySwapRef,
    pub latest_pred: PredictionRef,
    pub signal_ctx: strategy::SignalContext,
    pub coordinator_tx: Sender<RiskCheckRequest>,
    pub feature_tx: Sender<FeatureVector>,
    pub kill_switch: Arc<KillSwitch>,
    pub metrics: Arc<GlobalMetrics>,
    pub prediction_staleness_ns: u64,
}

impl AssetProcessor {
    /// Fast precheck on the hot path: returns `true` if the intent should proceed to the channel.
    /// Combines kill-switch and staleness checks to avoid wasted channel bandwidth.
    #[inline]
    fn fast_precheck(&self, prediction: &Prediction) -> bool {
        if self.kill_switch.is_active() {
            return false;
        }
        if prediction.is_stale(self.prediction_staleness_ns) {
            self.metrics.dropped_intents.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        true
    }

    pub fn run_loop(&mut self, md_rx: &Receiver<RawTick>) {
        let mut batch = Vec::with_capacity(32);
        let mut current_spread_bps = 0.0;

        while !self.kill_switch.is_active() {
            let _count = recv_batch(md_rx, &mut batch, 32);

            for tick in batch.drain(..) {
                let normalized = self.normalizer.process(tick.clone());
                let features = self.feature_engine.compute(&normalized);
                let _ = self.feature_tx.try_send(features.clone());

                self.signal_ctx.update_price(normalized.mid_price);
                if normalized.mid_price > 0.0 {
                    current_spread_bps = (normalized.ask - normalized.bid) / normalized.mid_price * 10000.0;
                    self.signal_ctx.update_spread(current_spread_bps.max(0.0));
                }

                // Read prediction from ArcSwap (set by PredictionEngine)
                let prediction = self.latest_pred.load_full();
                
                // Fast precheck before channel send
                if !self.fast_precheck(&prediction) {
                    continue;
                }

                let strat = self.strategy.load_full();
                if let Some(signal) = strat.evaluate(&prediction, &self.signal_ctx) {
                    let request = self.build_risk_request(&signal, &prediction, current_spread_bps);
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

    pub fn build_risk_request(&self, signal: &TradeIntent, prediction: &Prediction, current_spread_bps: f64) -> RiskCheckRequest {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        // Calculate implied volatility from prediction confidence (simplified)
        let current_volatility = (1.0 - prediction.confidence as f64).max(0.0);

        RiskCheckRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            symbol: signal.symbol.clone(),
            intent_id: signal.intent_id.clone(),
            side: format!("{:?}", signal.side),
            quantity: 1.0,
            price: 150.0,
            timestamp_ns: now,
            current_volatility,
            current_spread_bps,
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
        let threading_config = config.threading_config.clone();
        
        let journal = JournalWriter::new(
            &config.journal_config.journal_dir,
            config.journal_config.flush_interval_ms,
            Arc::clone(&self.metrics),
            threading_config.journal_core_id,
        );
        self.journal = Some(journal);

        let heartbeat_monitor = ThreadHeartbeatMonitor::new(
            Arc::clone(&self.kill_switch),
            Arc::clone(&self.metrics),
            threading_config.heartbeat_timeout_ns,
            threading_config.heartbeat_check_interval_ms,
            threading_config.heartbeat_core_id,
        );
        self.heartbeat_monitor = Some(heartbeat_monitor);

        let symbols: Vec<String> = config.asset_configs
            .iter()
            .filter(|c| c.enabled)
            .map(|c| c.symbol.clone())
            .collect();
        
        let alpaca_core_id = threading_config.alpaca_feed_core_id;
        drop(config);

        let feed_rx = self.start_alpaca_feed(&symbols, alpaca_core_id);

        self.start_assets(feed_rx);
        self.start_config_watcher();
        self.start_command_actor();

        tracing::info!("Unified Trading Engine running");
    }

    fn start_alpaca_feed(&self, symbols: &[String], core_id: usize) -> Option<Receiver<RawTick>> {
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
        let hb_monitor = self.heartbeat_monitor.as_ref().map(|m| m.register_thread("alpaca-feed"));

        spawn_pinned(
            "alpaca-feed",
            core_id,
            ThreadPriority::Normal,
            move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create tokio runtime");

                rt.block_on(async {
                    let handle = feed.start();
                    while !ks.is_active() {
                        if let Some(ref hb) = hb_monitor {
                            hb.pulse();
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                    feed.stop();
                    let _ = handle.await;
                    tracing::info!("Alpaca feed stopped");
                });
            },
        );

        tracing::info!("Alpaca WebSocket feed started for {:?} on core {}", symbols, core_id);
        Some(feed_rx)
    }

    fn start_assets(&mut self, feed_rx: Option<Receiver<RawTick>>) {
        let mut md_tx_list: Vec<(String, Sender<RawTick>)> = Vec::new();
        let position_manager = Arc::new(PositionManager::new());
        let (lifecycle_tx, lifecycle_rx) = bounded::<OrderLifecycleEvent>(1000);

        let config = self.config.read();
        let threading_config = config.threading_config.clone();
        let broker = &config.broker_config;
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

        // Start lifecycle handler with heartbeat
        let pm_clone = Arc::clone(&position_manager);
        let ks = Arc::clone(&self.kill_switch);
        let metrics = Arc::clone(&self.metrics);
        let hb_lifecycle = self.heartbeat_monitor.as_ref().map(|m| m.register_thread("lifecycle-handler"));
        spawn_pinned(
            "lifecycle-handler",
            0, // Use core 0 for lifecycle handler
            ThreadPriority::Normal,
            move || {
                tracing::info!("Order lifecycle handler started");
                while !ks.is_active() {
                    if let Some(ref hb) = hb_lifecycle {
                        hb.pulse();
                    }
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
            },
        );

        let mut asset_idx = 0;
        for asset_config in &config.asset_configs {
            if !asset_config.enabled {
                continue;
            }

            let core_id = self.next_asset_core;
            self.next_asset_core += 1;
            if self.next_asset_core > 3 {
                self.next_asset_core = 1;
            }

            // Distribute per-asset threads across cores to avoid collision
            let pred_core_id = (threading_config.prediction_core_id + asset_idx) % 4;
            let risk_core_id = (threading_config.risk_core_id + asset_idx) % 4;
            let exec_core_id = (threading_config.execution_core_id + asset_idx) % 4;
            asset_idx += 1;

            let (md_tx, md_rx) = bounded::<RawTick>(10_000);
            let (feature_tx, feature_rx) = bounded::<FeatureVector>(1000);
            let (risk_tx, risk_rx) = bounded::<RiskCheckRequest>(1000);
            let (decision_tx, decision_rx) = bounded::<RiskDecision>(1000);
            let (lifecycle_tx_clone, _order_rx) = (lifecycle_tx.clone(), bounded::<String>(1000).1);

            md_tx_list.push((asset_config.symbol.clone(), md_tx));

            let ks = Arc::clone(&self.kill_switch);
            let metrics = Arc::clone(&self.metrics);
            let pm = Arc::clone(&position_manager);

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

            // Create prediction engine first to get the ArcSwap
            let pred_engine = PredictionEngine::new(feature_rx, &asset_config.symbol);
            let latest_pred = Arc::clone(&pred_engine.latest_pred);

            let processor = AssetProcessor {
                symbol: asset_config.symbol.clone(),
                normalizer: Normalizer::new(&asset_config.symbol),
                feature_engine: FeatureEngine::new(
                    &asset_config.symbol,
                    config.feature_config.rsi_period,
                    config.feature_config.atr_period,
                    config.feature_config.macd_signal,
                    20,
                ),
                strategy: strategy_arc,
                latest_pred,
                signal_ctx,
                coordinator_tx: risk_tx,
                feature_tx: feature_tx.clone(),
                kill_switch: Arc::clone(&ks),
                metrics: Arc::clone(&metrics),
                prediction_staleness_ns: config.strategy_config.prediction_staleness_ns,
            };

            let inference_engine = InferenceEngine::new(config.model_config.feature_vector_size);
            let _pred_handle = pred_engine.start(move |features| {
                inference_engine.predict(features)
            }, pred_core_id);

            let risk_coordinator = RiskCoordinator::new(
                risk_rx,
                decision_tx,
                config.risk_config.clone(),
                100_000.0,
                Arc::clone(&ks),
                Arc::clone(&metrics),
            );
            let _risk_handle = risk_coordinator.start(risk_core_id);

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
            let _exec_handle = exec_manager.start(exec_core_id);

            // Start asset processor with heartbeat
            let sym = asset_config.symbol.clone();
            let hb_processor = self.heartbeat_monitor.as_ref().map(|m| m.register_thread(&format!("asset-{}", sym)));
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

            tracing::info!(
                "Started asset processor for {} on core {} (prediction on core {} BELOW_NORMAL, risk on core {} HIGH, exec on core {} HIGH)",
                asset_config.symbol, core_id, pred_core_id, risk_core_id, exec_core_id
            );
        }
        drop(config);

        if let Some(feed_rx) = feed_rx {
            let tick_reactor_core_id = threading_config.tick_reactor_core_id;
            let (reactor_tx, reactor_handle) = spawn_reactor(
                feed_rx,
                Arc::clone(&self.kill_switch),
                Arc::clone(&self.metrics),
                tick_reactor_core_id,
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
        let config = self.config.read();
        let command_core_id = config.threading_config.command_core_id;
        drop(config);

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
                ControlCommand::SwapStrategy { symbol, strategy_type, params } => {
                    let registry = strategy_registry.lock().unwrap();
                    if let Some(strategy_ref) = registry.get(&symbol) {
                        let old_name = strategy_ref.load().name().to_string();
                        
                        // Use provided params or defaults based on strategy type
                        let (long_entry, short_entry, confidence, deadband, entry_cooldown, exit_cooldown, staleness, allow_short) = 
                            match params {
                                Some(p) => (
                                    p.long_entry_threshold,
                                    p.short_entry_threshold,
                                    p.confidence_minimum,
                                    p.hysteresis_deadband,
                                    p.entry_cooldown_ms,
                                    p.exit_cooldown_ms,
                                    p.prediction_staleness_ns,
                                    p.allow_short,
                                ),
                                None => match strategy_type.as_str() {
                                    "hysteresis" => (0.6, -0.6, 0.5, 0.15, 5000, 2000, 150_000_000, true),
                                    "conservative" => (0.8, -0.8, 0.6, 0.2, 10000, 5000, 200_000_000, false),
                                    "aggressive" => (0.4, -0.4, 0.3, 0.1, 2000, 1000, 100_000_000, true),
                                    _ => {
                                        return ControlResponse::Error(format!("Unknown strategy type: {}", strategy_type));
                                    }
                                }
                            };
                        
                        let new_strategy: Box<dyn strategy::Strategy> = Box::new(
                            StrategyEngine::new(&symbol, long_entry, short_entry, confidence, deadband, 
                                entry_cooldown, exit_cooldown, staleness, allow_short)
                        );
                        strategy_ref.store(Arc::new(new_strategy));
                        tracing::info!(symbol = %symbol, from = %old_name, to = %strategy_type, "Strategy swapped");
                        ControlResponse::Ok
                    } else {
                        ControlResponse::Error(format!("Symbol not found: {}", symbol))
                    }
                }
                _ => ControlResponse::Ok,
            }
        }, command_core_id);
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
