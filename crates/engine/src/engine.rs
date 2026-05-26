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
use unified_trading_core::idempotency::IdempotencyStore;
use parking_lot::RwLock;

use market_data::{Normalizer, RawTick};
use feature::{FeatureEngine, FeatureVector};
use model::{PredictionEngine, InferenceEngine};
use strategy::{StrategyEngine, TradeIntent};
use risk::{RiskCoordinator, RiskCheckRequest, RiskDecision};
use execution::{ExecutionManager, OrderLifecycleEvent, OrderTracker, RateLimiter};
use gateway::{AlpacaFeedConfig, AlpacaWebSocketFeed, AlpacaExecutionPort, MockExecutionPort, IExecutionPort, CircuitBreaker};

use crate::tick_reactor::{spawn_reactor, ReactorCommand};

#[derive(Clone)]
pub struct ExecutionSharedState {
    pub order_tracker: Arc<std::sync::Mutex<OrderTracker>>,
    pub rate_limiter: Arc<std::sync::Mutex<RateLimiter>>,
    pub circuit_breaker: Arc<CircuitBreaker>,
    pub idempotency_store: Arc<IdempotencyStore>,
}

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

pub type StrategySwapRef = Arc<ArcSwap<Box<dyn strategy::Strategy>>>;
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
    pub default_order_quantity: f64,
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
                let tick_start = std::time::Instant::now();
                let (normalized, gap) = self.normalizer.process(tick.clone());
                if gap {
                    self.metrics.feed_gaps.fetch_add(1, Ordering::Relaxed);
                }
                let features = self.feature_engine.compute(&normalized);
                let _ = self.feature_tx.try_send(features.clone());
                self.metrics.feature_channel_depth.fetch_add(1, Ordering::Relaxed);

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
                    let request = self.build_risk_request(&signal, &prediction, current_spread_bps, normalized.mid_price);
                    match self.coordinator_tx.try_send(request) {
                        Ok(()) => {
                            self.metrics.intents_generated.fetch_add(1, Ordering::Relaxed);
                            self.metrics.risk_channel_depth.fetch_add(1, Ordering::Relaxed);
                            let elapsed_ns = tick_start.elapsed().as_nanos() as u64;
                            self.metrics.tick_to_intent_latency.record(elapsed_ns);
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
                self.metrics.features_computed.fetch_add(1, Ordering::Relaxed);
                self.metrics.increment_per_symbol_tick(&self.symbol);
                self.metrics.increment_per_symbol_feature(&self.symbol);

                // Record feed latency (tick timestamp to processing time)
                let proc_ns = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;
                let latency = proc_ns.saturating_sub(tick.timestamp_ns);
                self.metrics.feed_latency.record(latency);
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

    pub fn build_risk_request(&self, signal: &TradeIntent, prediction: &Prediction, current_spread_bps: f64, mid_price: f64) -> RiskCheckRequest {
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
            quantity: self.default_order_quantity,
            price: mid_price,
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
    pub journal_tx: Option<crossbeam_channel::Sender<unified_trading_core::JournalCommand>>,
    pub heartbeat_monitor: Option<ThreadHeartbeatMonitor>,
    pub heartbeats: Option<Arc<parking_lot::RwLock<HashMap<String, Arc<std::sync::atomic::AtomicU64>>>>>,
    pub config_watcher: Option<ConfigWatcher>,
    pub strategy_registry: Arc<Mutex<HashMap<String, StrategySwapRef>>>,
    pub position_manager: Arc<PositionManager>,
    pub execution_states: HashMap<String, ExecutionSharedState>,
    pub tick_reactor_tx: Option<crossbeam_channel::Sender<crate::tick_reactor::ReactorCommand>>,
    next_asset_core: usize,
}

impl UnifiedEngine {
    pub fn new(config: EngineConfig) -> Self {
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        let command_channel = CommandChannel::new(config.channel_config.command_channel_capacity);
        let strategy_registry = Arc::new(Mutex::new(HashMap::new()));
        let config = Arc::new(RwLock::new(config));
        let position_manager = Arc::new(PositionManager::new());

        Self {
            config: Arc::clone(&config),
            kill_switch,
            metrics,
            command_channel,
            journal: None,
            journal_tx: None,
            heartbeat_monitor: None,
            heartbeats: None,
            config_watcher: None,
            strategy_registry,
            position_manager,
            execution_states: HashMap::new(),
            tick_reactor_tx: None,
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
        self.journal_tx = Some(journal.tx.clone());
        self.journal = Some(journal);

        let heartbeat_monitor = ThreadHeartbeatMonitor::new(
            Arc::clone(&self.kill_switch),
            Arc::clone(&self.metrics),
            threading_config.heartbeat_timeout_ns,
            threading_config.heartbeat_check_interval_ms,
            threading_config.heartbeat_core_id,
        );
        self.heartbeats = Some(heartbeat_monitor.heartbeats());
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
        let config = self.config.read();
        let channel_cfg = config.channel_config.clone();
        let (lifecycle_tx, lifecycle_rx) = bounded::<OrderLifecycleEvent>(channel_cfg.lifecycle_channel_capacity);

        let threading_config = config.threading_config.clone();
        let broker = &config.broker_config;
        let execution_defaults = config.execution_defaults.clone();
        let circuit_breaker_cfg = config.circuit_breaker_config.clone();
        let reactor_cfg = config.reactor_config.clone();
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
        let pm_clone = Arc::clone(&self.position_manager);
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
                            metrics.lifecycle_channel_depth.fetch_sub(1, Ordering::Relaxed);
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

            let (md_tx, md_rx) = bounded::<RawTick>(channel_cfg.per_asset_tick_channel_capacity);
            let (feature_tx, feature_rx) = bounded::<FeatureVector>(channel_cfg.feature_channel_capacity);
            let (risk_tx, risk_rx) = bounded::<RiskCheckRequest>(channel_cfg.risk_channel_capacity);
            let (decision_tx, decision_rx) = bounded::<RiskDecision>(channel_cfg.decision_channel_capacity);
            let (lifecycle_tx_clone, _order_rx) = (lifecycle_tx.clone(), bounded::<String>(channel_cfg.lifecycle_channel_capacity).1);

            md_tx_list.push((asset_config.symbol.clone(), md_tx));

            let ks = Arc::clone(&self.kill_switch);
            let metrics = Arc::clone(&self.metrics);
            let pm = Arc::clone(&self.position_manager);

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
                config.strategy_config.trade_intent_ttl_ns,
                config.strategy_config.max_long_units,
                config.strategy_config.max_short_units,
                config.strategy_config.urgency_aggressive_threshold,
                config.strategy_config.urgency_normal_threshold,
                config.model_config.action_score_rsi_weight,
                config.model_config.action_score_macd_weight,
                config.model_config.action_score_volatility_weight,
                config.model_config.atr_penalty_threshold,
                config.model_config.atr_penalty_value,
                config.model_config.rsi_overbought,
                config.model_config.rsi_oversold,
                config.model_config.rsi_neutral,
                config.model_config.confidence_rsi_weight,
                config.model_config.confidence_macd_weight,
                config.model_config.confidence_regime_weight,
                config.feature_config.volume_ratio_clamp,
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
                    config.feature_config.feature_capacity,
                    config.feature_config.price_window_size,
                    config.feature_config.volume_window_size,
                    config.feature_config.spread_window_size,
                    config.feature_config.return_1_window,
                    config.feature_config.return_5_window,
                    config.feature_config.return_20_window,
                    config.feature_config.volume_ratio_clamp,
                    config.feature_config.regime_volatile_atr_threshold,
                    config.feature_config.regime_strength_atr_divisor,
                    config.feature_config.regime_trending_threshold,
                ),
                strategy: strategy_arc,
                latest_pred,
                signal_ctx,
                coordinator_tx: risk_tx,
                feature_tx: feature_tx.clone(),
                kill_switch: Arc::clone(&ks),
                metrics: Arc::clone(&metrics),
                prediction_staleness_ns: config.strategy_config.prediction_staleness_ns,
                default_order_quantity: execution_defaults.default_order_quantity,
            };

            let inference_engine = InferenceEngine::new(
                config.model_config.feature_vector_size,
                config.model_config.action_score_rsi_weight,
                config.model_config.action_score_macd_weight,
                config.model_config.action_score_volatility_weight,
                config.model_config.atr_penalty_threshold,
                config.model_config.atr_penalty_value,
                config.model_config.rsi_overbought,
                config.model_config.rsi_oversold,
                config.model_config.rsi_neutral,
                config.model_config.forecast_momentum_weight,
                config.model_config.forecast_volume_weight,
                config.feature_config.volume_ratio_clamp,
                config.model_config.volume_confirmation_threshold,
            );
            let _pred_handle = pred_engine.start(move |features| {
                inference_engine.predict(features)
            }, pred_core_id);

            let risk_coordinator = RiskCoordinator::new(
                risk_rx,
                decision_tx,
                config.risk_config.clone(),
                config.risk_config.initial_equity,
                Arc::clone(&ks),
                Arc::clone(&metrics),
            );
            let _risk_handle = risk_coordinator.start(risk_core_id);

            let global_rate = config.risk_config.max_order_rate_per_sec as f64;
            let per_symbol_rate = global_rate / execution_defaults.execution_per_symbol_rate_divisor;
            let order_tracker = Arc::new(std::sync::Mutex::new(OrderTracker::new()));
            let rate_limiter = Arc::new(std::sync::Mutex::new(RateLimiter::new(global_rate, per_symbol_rate)));
            let circuit_breaker = Arc::new(CircuitBreaker::new(circuit_breaker_cfg.failure_threshold, circuit_breaker_cfg.cooldown_ms));
            let idempotency_store = Arc::new(IdempotencyStore::new());
            let exec_shared = ExecutionSharedState {
                order_tracker: Arc::clone(&order_tracker),
                rate_limiter: Arc::clone(&rate_limiter),
                circuit_breaker: Arc::clone(&circuit_breaker),
                idempotency_store: Arc::clone(&idempotency_store),
            };
            self.execution_states.insert(asset_config.symbol.clone(), exec_shared);

            let exec_manager = ExecutionManager::new(
                decision_rx,
                lifecycle_tx_clone,
                Arc::clone(&execution_port),
                global_rate,
                per_symbol_rate,
                Arc::clone(&metrics),
                Arc::clone(&ks),
                pm,
                order_tracker,
                rate_limiter,
                circuit_breaker,
                idempotency_store,
                unified_trading_core::validator::RequestValidator::new(config.validator_config.clone()),
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
                reactor_cfg.max_batch_size,
                reactor_cfg.control_batch_size,
                reactor_cfg.sleep_on_empty_us,
                reactor_cfg.backpressure_log_interval,
            );

            self.tick_reactor_tx = Some(reactor_tx.clone());

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
        let metrics_for_actor = Arc::clone(&metrics);
        let kill_switch = Arc::clone(&self.kill_switch);
        let strategy_registry = Arc::clone(&self.strategy_registry);
        let config_arc = Arc::clone(&self.config);
        let execution_states = Arc::new(std::sync::Mutex::new(self.execution_states.clone()));
        let position_manager = Arc::clone(&self.position_manager);
        let journal_tx_opt = self.journal_tx.clone();
        let tick_reactor_tx_opt = self.tick_reactor_tx.clone();

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

                        let cfg = config_arc.read();
                        let model_cfg = cfg.model_config.clone();
                        let feature_cfg = cfg.feature_config.clone();
                        drop(cfg);

                        let (
                            long_entry, short_entry, confidence, deadband,
                            entry_cooldown, exit_cooldown, staleness, allow_short,
                            trade_intent_ttl_ns, max_long_units, max_short_units,
                            urgency_aggressive_threshold, urgency_normal_threshold,
                        ) = match params {
                            Some(p) => (
                                p.long_entry_threshold,
                                p.short_entry_threshold,
                                p.confidence_minimum,
                                p.hysteresis_deadband,
                                p.entry_cooldown_ms,
                                p.exit_cooldown_ms,
                                p.prediction_staleness_ns,
                                p.allow_short,
                                p.trade_intent_ttl_ns,
                                p.max_long_units,
                                p.max_short_units,
                                p.urgency_aggressive_threshold as f64,
                                p.urgency_normal_threshold as f64,
                            ),
                            None => match strategy_type.as_str() {
                                "hysteresis" => (0.6, -0.6, 0.5, 0.15, 5000, 2000, 150_000_000, true, 30_000_000_000, 100.0, 100.0, 0.85, 0.5),
                                "conservative" => (0.8, -0.8, 0.6, 0.2, 10000, 5000, 200_000_000, false, 60_000_000_000, 50.0, 50.0, 0.9, 0.6),
                                "aggressive" => (0.4, -0.4, 0.3, 0.1, 2000, 1000, 100_000_000, true, 15_000_000_000, 200.0, 200.0, 0.75, 0.4),
                                _ => {
                                    return ControlResponse::Error(format!("Unknown strategy type: {}", strategy_type));
                                }
                            }
                        };

                        let new_strategy: Box<dyn strategy::Strategy> = Box::new(
                            StrategyEngine::new(
                                &symbol, long_entry, short_entry, confidence, deadband,
                                entry_cooldown, exit_cooldown, staleness, allow_short,
                                trade_intent_ttl_ns, max_long_units, max_short_units,
                                urgency_aggressive_threshold, urgency_normal_threshold,
                                model_cfg.action_score_rsi_weight,
                                model_cfg.action_score_macd_weight,
                                model_cfg.action_score_volatility_weight,
                                model_cfg.atr_penalty_threshold,
                                model_cfg.atr_penalty_value,
                                model_cfg.rsi_overbought,
                                model_cfg.rsi_oversold,
                                model_cfg.rsi_neutral,
                                model_cfg.confidence_rsi_weight,
                                model_cfg.confidence_macd_weight,
                                model_cfg.confidence_regime_weight,
                                feature_cfg.volume_ratio_clamp,
                            )
                        );
                        strategy_ref.store(Arc::new(new_strategy));
                        tracing::info!(symbol = %symbol, from = %old_name, to = %strategy_type, "Strategy swapped");
                        ControlResponse::Ok
                    } else {
                        ControlResponse::Error(format!("Symbol not found: {}", symbol))
                    }
                }
                ControlCommand::PauseAsset(sym) => {
                    let mut cfg = config_arc.write();
                    if let Some(asset) = cfg.asset_configs.iter_mut().find(|a| a.symbol == sym) {
                        asset.enabled = false;
                        tracing::info!(symbol = %sym, "Asset paused");
                        ControlResponse::Ok
                    } else {
                        ControlResponse::Error(format!("Symbol not found: {}", sym))
                    }
                }
                ControlCommand::ResumeAsset(sym) => {
                    let mut cfg = config_arc.write();
                    if let Some(asset) = cfg.asset_configs.iter_mut().find(|a| a.symbol == sym) {
                        asset.enabled = true;
                        tracing::info!(symbol = %sym, "Asset resumed");
                        ControlResponse::Ok
                    } else {
                        ControlResponse::Error(format!("Symbol not found: {}", sym))
                    }
                }
                ControlCommand::SetMode(mode) => {
                    let mut cfg = config_arc.write();
                    cfg.broker_config.paper_trading = mode == "paper";
                    tracing::info!(mode = %mode, "Trading mode updated");
                    ControlResponse::Ok
                }
                ControlCommand::SetRiskParams(update) => {
                    let mut cfg = config_arc.write();
                    cfg.risk_config.max_portfolio_exposure = update.max_portfolio_exposure;
                    cfg.risk_config.max_leverage = update.max_leverage;
                    cfg.risk_config.max_drawdown_pct = update.max_drawdown_pct;
                    cfg.risk_config.max_order_rate_per_sec = update.max_order_rate_per_sec;
                    cfg.risk_config.max_position_per_symbol = update.max_position_per_symbol;
                    cfg.risk_config.max_volatility = update.max_volatility;
                    cfg.risk_config.max_spread_bps = update.max_spread_bps;
                    cfg.risk_config.max_slippage_bps = update.max_slippage_bps;
                    cfg.risk_config.allow_short = update.allow_short;
                    cfg.risk_config.kill_switch_on_drawdown = update.kill_switch_on_drawdown;
                    tracing::info!("Risk parameters updated via API");
                    ControlResponse::Ok
                }
                ControlCommand::SetBrokerParams(update) => {
                    let mut cfg = config_arc.write();
                    cfg.broker_config.broker_type = update.broker_type;
                    cfg.broker_config.paper_trading = update.paper_trading;
                    cfg.broker_config.ws_url = update.ws_url;
                    cfg.broker_config.rest_url = update.rest_url;
                    cfg.broker_config.max_retries = update.max_retries;
                    cfg.broker_config.retry_backoff_ms = update.retry_backoff_ms;
                    tracing::info!("Broker parameters updated via API");
                    ControlResponse::Ok
                }
                ControlCommand::SetFeatureParams(update) => {
                    let mut cfg = config_arc.write();
                    cfg.feature_config.rsi_period = update.rsi_period;
                    cfg.feature_config.macd_fast = update.macd_fast;
                    cfg.feature_config.macd_slow = update.macd_slow;
                    cfg.feature_config.macd_signal = update.macd_signal;
                    cfg.feature_config.atr_period = update.atr_period;
                    cfg.feature_config.ema_periods = update.ema_periods;
                    cfg.feature_config.rolling_window_sizes = update.rolling_window_sizes;
                    tracing::info!("Feature parameters updated via API");
                    ControlResponse::Ok
                }
                ControlCommand::SetModelParams(update) => {
                    let mut cfg = config_arc.write();
                    cfg.model_config.model_dir = update.model_dir;
                    cfg.model_config.inference_threads = update.inference_threads;
                    cfg.model_config.max_inference_latency_ms = update.max_inference_latency_ms;
                    cfg.model_config.feature_vector_size = update.feature_vector_size;
                    tracing::info!("Model parameters updated via API");
                    ControlResponse::Ok
                }
                ControlCommand::SetJournalParams(update) => {
                    let mut cfg = config_arc.write();
                    cfg.journal_config.journal_dir = update.journal_dir;
                    cfg.journal_config.flush_interval_ms = update.flush_interval_ms;
                    cfg.journal_config.snapshot_interval_sec = update.snapshot_interval_sec;
                    cfg.journal_config.max_file_size_mb = update.max_file_size_mb;
                    tracing::info!("Journal parameters updated via API");
                    ControlResponse::Ok
                }
                ControlCommand::SetAssetConfig { symbol, config: asset_update } => {
                    let mut cfg = config_arc.write();
                    if let Some(asset) = cfg.asset_configs.iter_mut().find(|a| a.symbol == symbol) {
                        asset.enabled = asset_update.enabled;
                        asset.max_position = asset_update.max_position;
                        asset.tick_size = asset_update.tick_size;
                        tracing::info!(symbol = %symbol, "Asset config updated via API");
                        ControlResponse::Ok
                    } else {
                        ControlResponse::Error(format!("Symbol not found: {}", symbol))
                    }
                }
                ControlCommand::SetExecutionDefaults(update) => {
                    let mut cfg = config_arc.write();
                    cfg.execution_defaults.default_order_quantity = update.default_order_quantity;
                    cfg.execution_defaults.execution_per_symbol_rate_divisor = update.execution_per_symbol_rate_divisor;
                    tracing::info!("Execution defaults updated via API");
                    ControlResponse::Ok
                }
                ControlCommand::SetCircuitBreakerParams(update) => {
                    let states = execution_states.lock().unwrap();
                    for (_, exec_state) in states.iter() {
                        exec_state.circuit_breaker.set_failure_threshold(update.failure_threshold);
                        exec_state.circuit_breaker.set_cooldown_ms(update.cooldown_ms);
                    }
                    let mut cfg = config_arc.write();
                    cfg.circuit_breaker_config.failure_threshold = update.failure_threshold;
                    cfg.circuit_breaker_config.cooldown_ms = update.cooldown_ms;
                    tracing::info!("Circuit breaker config updated via API");
                    ControlResponse::Ok
                }
                ControlCommand::SetRateLimits(update) => {
                    let states = execution_states.lock().unwrap();
                    for (_, exec_state) in states.iter() {
                        let mut rl = exec_state.rate_limiter.lock().unwrap();
                        rl.set_global_rate(update.global_rate);
                        rl.set_default_per_symbol_rate(update.per_symbol_rate);
                    }
                    tracing::info!("Rate limits updated via API");
                    ControlResponse::Ok
                }
                ControlCommand::SetChannelParams(update) => {
                    let mut cfg = config_arc.write();
                    cfg.channel_config.per_asset_tick_channel_capacity = update.per_asset_tick_channel_capacity;
                    cfg.channel_config.feature_channel_capacity = update.feature_channel_capacity;
                    cfg.channel_config.risk_channel_capacity = update.risk_channel_capacity;
                    cfg.channel_config.decision_channel_capacity = update.decision_channel_capacity;
                    cfg.channel_config.lifecycle_channel_capacity = update.lifecycle_channel_capacity;
                    cfg.channel_config.command_channel_capacity = update.command_channel_capacity;
                    cfg.channel_config.journal_channel_capacity = update.journal_channel_capacity;
                    tracing::info!("Channel config updated via API");
                    ControlResponse::Ok
                }
                ControlCommand::SetReactorParams(update) => {
                    let mut cfg = config_arc.write();
                    cfg.reactor_config.max_batch_size = update.max_batch_size;
                    cfg.reactor_config.control_batch_size = update.control_batch_size;
                    cfg.reactor_config.sleep_on_empty_us = update.sleep_on_empty_us;
                    cfg.reactor_config.backpressure_log_interval = update.backpressure_log_interval;
                    tracing::info!("Reactor config updated via API");
                    ControlResponse::Ok
                }
                ControlCommand::SetValidatorParams(update) => {
                    let mut cfg = config_arc.write();
                    cfg.validator_config.max_symbol_length = update.max_symbol_length;
                    cfg.validator_config.max_quantity = update.max_quantity;
                    cfg.validator_config.max_order_id_length = update.max_order_id_length;
                    tracing::info!("Validator config updated via API");
                    ControlResponse::Ok
                }
                ControlCommand::CircuitBreakerTrip => {
                    let states = execution_states.lock().unwrap();
                    for (_, exec_state) in states.iter() {
                        exec_state.circuit_breaker.trip();
                    }
                    ControlResponse::Ok
                }
                ControlCommand::CircuitBreakerReset => {
                    let states = execution_states.lock().unwrap();
                    for (_, exec_state) in states.iter() {
                        exec_state.circuit_breaker.reset();
                    }
                    ControlResponse::Ok
                }
                ControlCommand::ReloadConfig => {
                    tracing::info!("Config reload requested via API (filesystem watcher handles live reload)");
                    ControlResponse::Ok
                }
                ControlCommand::FlushJournal => {
                    if let Some(ref tx) = journal_tx_opt {
                        let (ack_tx, ack_rx) = crossbeam_channel::bounded::<()>(1);
                        if let Err(_) = tx.send(unified_trading_core::JournalCommand::Flush { ack: ack_tx }) {
                            tracing::warn!("Journal channel closed");
                        } else if let Err(_) = ack_rx.recv_timeout(std::time::Duration::from_secs(5)) {
                            tracing::warn!("Journal flush timeout");
                        }
                    }
                    ControlResponse::Ok
                }
                ControlCommand::ModelSwap(model_id) => {
                    tracing::info!(model_id = %model_id, "Model swap requested via API (not yet implemented)");
                    ControlResponse::Ok
                }
                ControlCommand::UpdateConfig(path) => {
                    tracing::info!(path = %path, "UpdateConfig requested via API");
                    ControlResponse::Ok
                }
                ControlCommand::SubscribeFeed { symbol } => {
                    if let Some(ref reactor_tx) = tick_reactor_tx_opt {
                        let (md_tx, md_rx) = bounded::<RawTick>(config_arc.read().channel_config.per_asset_tick_channel_capacity);
                        let _ = reactor_tx.send(ReactorCommand::Subscribe { symbol: symbol.clone(), tx: md_tx });
                        tracing::info!(symbol = %symbol, "Subscribe feed sent to tick reactor");
                        ControlResponse::Ok
                    } else {
                        ControlResponse::Error("Tick reactor not running".to_string())
                    }
                }
                ControlCommand::UnsubscribeFeed { symbol } => {
                    if let Some(ref reactor_tx) = tick_reactor_tx_opt {
                        let _ = reactor_tx.send(ReactorCommand::Unsubscribe { symbol: symbol.clone() });
                        tracing::info!(symbol = %symbol, "Unsubscribe feed sent to tick reactor");
                        ControlResponse::Ok
                    } else {
                        ControlResponse::Error("Tick reactor not running".to_string())
                    }
                }
            }
        }, command_core_id, Some(metrics_for_actor));
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
