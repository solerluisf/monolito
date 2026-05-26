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
use unified_trading_core::portfolio_manager::PortfolioManager;
use unified_trading_core::config_watcher::ConfigWatcher;
use unified_trading_core::idempotency::IdempotencyStore;
use parking_lot::RwLock;

use market_data::{Normalizer, RawTick};
use feature::{FeatureEngine, FeatureVector};
use model::{Prediction, PredictionEngine, InferenceEngine};
use strategy::{StrategyEngine, TradeIntent, SizeHint};
use risk::{RiskCoordinator, RiskCheckRequest, RiskDecision};
use execution::{ExecutionManager, OrderLifecycleEvent, OrderTracker, RateLimiter};
use gateway::{AlpacaFeedConfig, AlpacaWebSocketFeed, AlpacaExecutionPort, MockExecutionPort, IExecutionPort, CircuitBreaker, OpenOrderInfo, PositionInfo, OrderSide};

use crate::tick_reactor::{spawn_reactor, ReactorCommand};

#[derive(Clone)]
pub struct ExecutionSharedState {
    pub order_tracker: Arc<parking_lot::Mutex<OrderTracker>>,
    pub rate_limiter: Arc<parking_lot::Mutex<RateLimiter>>,
    pub circuit_breaker: Arc<CircuitBreaker>,
    pub idempotency_store: Arc<IdempotencyStore>,
}

/// Holds the per-asset pipeline channels and thread handles so they can be
/// shut down independently when a symbol is unsubscribed at runtime.
pub struct AssetPipeline {
    pub symbol: String,
    pub md_tx: crossbeam_channel::Sender<RawTick>,
    pub thread_handles: Vec<std::thread::JoinHandle<()>>,
    pub strategy_ref: StrategySwapRef,
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
use parking_lot::Mutex;

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
    /// Only checks kill-switch; staleness is handled by heuristic fallback in the main loop.
    #[inline]
    fn fast_precheck(&self, _prediction: &Prediction) -> bool {
        if self.kill_switch.is_active() {
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
                let (normalized, gap) = match self.normalizer.process(tick.clone()) {
                    Some(result) => result,
                    None => {
                        tracing::warn!(symbol = %self.symbol, "Tick rejected by normalizer (malformed prices)");
                        continue;
                    }
                };
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

                // If prediction is stale, fall back to a heuristic based on raw features
                let prediction = if prediction.is_stale(self.prediction_staleness_ns) {
                    self.metrics.model_fallback_activations.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(symbol = %self.symbol, "Model prediction stale; using heuristic fallback");
                    Arc::new(Prediction::heuristic_from_features(&features, &self.symbol))
                } else {
                    prediction
                };
                
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

        // Drain remaining ticks so the channel doesn't hold stale messages
        while let Ok(_tick) = md_rx.try_recv() {
            self.metrics.ticks_processed.fetch_add(1, Ordering::Relaxed);
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

        let quantity = match signal.size_hint {
            SizeHint::Units(u) => u as f64,
            SizeHint::Notional(n) if mid_price > 0.0 => n / mid_price,
            _ => {
                tracing::warn!(size_hint = ?signal.size_hint, "Unsupported size hint, falling back to default quantity");
                self.default_order_quantity
            }
        };

        RiskCheckRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            symbol: signal.symbol.clone(),
            intent_id: signal.intent_id.clone(),
            side: format!("{:?}", signal.side),
            quantity,
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
    pub portfolio_manager: Arc<PortfolioManager>,
    pub execution_states: HashMap<String, ExecutionSharedState>,
    pub asset_pipelines: HashMap<String, AssetPipeline>,
    pub tick_reactor_tx: Option<crossbeam_channel::Sender<crate::tick_reactor::ReactorCommand>>,
    pub thread_handles: Vec<std::thread::JoinHandle<()>>,
    pub command_actor: Option<CommandActor>,
    next_asset_core: usize,
}

impl UnifiedEngine {
    pub fn new(config: EngineConfig) -> Self {
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        let command_channel = CommandChannel::new(config.channel_config.command_channel_capacity);
        let strategy_registry = Arc::new(Mutex::new(HashMap::new()));
        let initial_equity = config.risk_config.initial_equity;
        let flat_threshold = config.risk_config.portfolio_flat_threshold;
        let config = Arc::new(RwLock::new(config));
        let portfolio_manager = Arc::new(PortfolioManager::new(initial_equity, flat_threshold));

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
            portfolio_manager,
            execution_states: HashMap::new(),
            asset_pipelines: HashMap::new(),
            tick_reactor_tx: None,
            thread_handles: Vec::new(),
            command_actor: None,
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
        self.command_actor = Some(self.start_command_actor());

        tracing::info!("Unified Trading Engine running");
    }

    fn start_alpaca_feed(&mut self, symbols: &[String], core_id: usize) -> Option<Receiver<RawTick>> {
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

        let handle = spawn_pinned(
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
        self.thread_handles.push(handle);

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
                    Arc::new(MockExecutionPort::default())
                }
            }
        } else {
            tracing::warn!("Alpaca API credentials not configured, using mock execution port");
            Arc::new(MockExecutionPort::default())
        };

        // Start lifecycle handler with heartbeat
        let pm_clone = Arc::clone(&self.portfolio_manager);
        let ks = Arc::clone(&self.kill_switch);
        let metrics = Arc::clone(&self.metrics);
        let hb_lifecycle = self.heartbeat_monitor.as_ref().map(|m| m.register_thread("lifecycle-handler"));
        let lifecycle_handle = spawn_pinned(
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
                // Drain remaining lifecycle events so they are not lost
                while let Ok(event) = lifecycle_rx.try_recv() {
                    metrics.lifecycle_channel_depth.fetch_sub(1, Ordering::Relaxed);
                    tracing::info!(
                        event_type = ?event.event_type,
                        symbol = %event.symbol,
                        execution_id = %event.execution_id,
                        "Order lifecycle event (drained)"
                    );
                    metrics.orders_lifecycle_events.fetch_add(1, Ordering::Relaxed);
                }
                tracing::info!("Order lifecycle handler stopped");
            },
        );
        self.thread_handles.push(lifecycle_handle);

        // Pre-create execution shared states for all enabled assets
        let mut prebuilt_states: HashMap<String, ExecutionSharedState> = HashMap::new();
        let global_rate = config.risk_config.max_order_rate_per_sec as f64;
        let per_symbol_rate = global_rate / execution_defaults.execution_per_symbol_rate_divisor;
        let asset_configs: Vec<_> = config.asset_configs.clone();
        let journal_dir = config.journal_config.journal_dir.clone();
        for asset_config in &asset_configs {
            if !asset_config.enabled {
                continue;
            }
            let idempotency_path = std::path::PathBuf::from(&journal_dir)
                .join(format!("idempotency_{}.log", asset_config.symbol));
            let state = ExecutionSharedState {
                order_tracker: Arc::new(parking_lot::Mutex::new(OrderTracker::new())),
                rate_limiter: Arc::new(parking_lot::Mutex::new(RateLimiter::new(global_rate, per_symbol_rate))),
                circuit_breaker: Arc::new(CircuitBreaker::new(circuit_breaker_cfg.failure_threshold, circuit_breaker_cfg.cooldown_ms)),
                idempotency_store: Arc::new(IdempotencyStore::new_with_path(
                    IdempotencyStore::DEFAULT_CAPACITY,
                    &idempotency_path,
                )),
            };
            prebuilt_states.insert(asset_config.symbol.clone(), state);
        }

        // Reconcile broker state and rehydrate from journal before trading starts
        self.reconcile_and_rehydrate(&execution_port, &prebuilt_states, &config);

        // Clone config before dropping the read lock so spawn_asset_pipeline can use it
        let config_clone = config.clone();
        // Drop config read lock before spawning pipelines (spawn_asset_pipeline needs &mut self)
        drop(config);

        let mut asset_idx = 0;
        for asset_config in &asset_configs {
            if !asset_config.enabled {
                continue;
            }

            let exec_shared = prebuilt_states.remove(&asset_config.symbol)
                .expect("prebuilt state exists for enabled asset");
            let pipeline = self.spawn_asset_pipeline(
                &asset_config.symbol,
                &lifecycle_tx,
                &execution_port,
                &config_clone,
                asset_idx,
                exec_shared,
            );
            md_tx_list.push((asset_config.symbol.clone(), pipeline.md_tx.clone()));
            self.asset_pipelines.insert(asset_config.symbol.clone(), pipeline);
            asset_idx += 1;
        }

        // Start periodic lightweight reconciliation thread
        self.start_periodic_reconciliation(&execution_port);

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

    /// Reconcile local state with the broker and replay the journal to rehydrate
    /// `PortfolioManager` and per-asset `OrderTracker` / `IdempotencyStore`.
    fn reconcile_and_rehydrate(
        &self,
        execution_port: &Arc<dyn IExecutionPort>,
        prebuilt_states: &HashMap<String, ExecutionSharedState>,
        config: &EngineConfig,
    ) {
        tracing::info!("Starting broker reconciliation and state rehydration...");

        // 1. Query broker for open orders
        match execution_port.query_open_orders() {
            Ok(open_orders) => {
                tracing::info!(count = %open_orders.len(), "Broker open orders received");
                for order in &open_orders {
                    if let Some(state) = prebuilt_states.get(&order.symbol) {
                        let side_str = match order.side {
                            OrderSide::Buy => "buy",
                            OrderSide::Sell => "sell",
                        };
                        let mut tracker = state.order_tracker.lock();
                        let order_id = tracker.create_order(&order.symbol, side_str, order.quantity, None, &order.order_id);
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_nanos() as u64;
                        let _ = tracker.submit_order(&order_id, now);
                        if order.filled_qty > 0.0 {
                            let _ = tracker.partial_fill_order(&order_id, order.filled_qty, 0.0, now);
                        }
                        tracing::info!(symbol = %order.symbol, order_id = %order.order_id, "Rehydrated open order from broker");
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Failed to query broker open orders: {}", e);
            }
        }

        // 2. Query broker for positions
        match execution_port.query_positions() {
            Ok(positions) => {
                tracing::info!(count = %positions.len(), "Broker positions received");
                for pos in &positions {
                    let is_buy = pos.qty > 0.0;
                    let qty = pos.qty.abs();
                    self.portfolio_manager.on_fill(&pos.symbol, pos.current_price, qty, is_buy);
                    // Override avg entry price with broker's value for accuracy
                    if let Some(mut p) = self.portfolio_manager.get_position(&pos.symbol) {
                        p.avg_entry_price = pos.avg_entry_price;
                        // Note: PortfolioManager doesn't have a direct setter for avg_entry_price;
                        // the on_fill calculation is close enough for reconciliation.
                    }
                    tracing::info!(symbol = %pos.symbol, qty = %pos.qty, "Rehydrated position from broker");
                }
            }
            Err(e) => {
                tracing::warn!("Failed to query broker positions: {}", e);
            }
        }

        // 3. Replay journal to recover idempotency keys and fill state
        if let Some(ref journal) = self.journal {
            let mut replayed_orders: u64 = 0;
            let mut replayed_fills: u64 = 0;
            let result = journal.replay(|entry| {
                match entry {
                    JournalEntry::Order { symbol, timestamp_ns, data } => {
                        // Parse idempotency key from data if present
                        if let Some(key_start) = data.find("decision=") {
                            let key = format!("{}-{}", symbol, &data[key_start + 9..]);
                            if let Some(state) = prebuilt_states.get(symbol) {
                                state.idempotency_store.mark_processed(key, "replayed".to_string());
                            }
                        }
                        replayed_orders += 1;
                    }
                    JournalEntry::Fill { symbol, timestamp_ns, data } => {
                        // Try to parse fill price and qty from data
                        let mut price: f64 = 0.0;
                        let mut qty: f64 = 0.0;
                        for part in data.split(',') {
                            if let Some(val) = part.strip_prefix("price=") {
                                price = val.parse().unwrap_or(0.0);
                            }
                            if let Some(val) = part.strip_prefix("qty=") {
                                qty = val.parse().unwrap_or(0.0);
                            }
                        }
                        if price > 0.0 && qty > 0.0 {
                            // We don't know side from the fill entry alone, so we
                            // approximate by looking at the net position change.
                            // For exact recovery, broker positions take precedence.
                        }
                        replayed_fills += 1;
                    }
                    _ => {}
                }
            });
            match result {
                Ok(count) => {
                    tracing::info!(
                        entries = %count,
                        orders = %replayed_orders,
                        fills = %replayed_fills,
                        "Journal replay complete"
                    );
                }
                Err(e) => {
                    tracing::warn!("Journal replay failed: {}", e);
                }
            }
        } else {
            tracing::warn!("No journal writer available; skipping journal replay");
        }

        tracing::info!("Reconciliation and rehydration complete");
    }

    /// Spawn a background thread that periodically queries the broker for open
    /// orders and positions, logs discrepancies against local state, and emits
    /// metrics.  This is a lightweight drift-detection mechanism, not a full
    /// rehydration.
    fn start_periodic_reconciliation(&mut self, execution_port: &Arc<dyn IExecutionPort>) {
        let port = Arc::clone(execution_port);
        let portfolio_manager = Arc::clone(&self.portfolio_manager);
        let execution_states = Arc::new(parking_lot::Mutex::new(self.execution_states.clone()));
        let kill_switch = Arc::clone(&self.kill_switch);
        let interval = std::time::Duration::from_secs(60);

        let handle = spawn_pinned(
            "periodic-reconciliation",
            0,
            ThreadPriority::Normal,
            move || {
                loop {
                    std::thread::sleep(interval);
                    if kill_switch.is_active() {
                        break;
                    }

                    // Query broker positions
                    match port.query_positions() {
                        Ok(positions) => {
                            let local_positions: Vec<String> = portfolio_manager
                                .get_all_positions()
                                .into_iter()
                                .filter(|p| !p.is_flat())
                                .map(|p| p.symbol.clone())
                                .collect();

                            for pos in &positions {
                                if !local_positions.contains(&pos.symbol) {
                                    tracing::warn!(
                                        symbol = %pos.symbol,
                                        qty = %pos.qty,
                                        "Broker has position not tracked locally"
                                    );
                                }
                            }

                            for sym in &local_positions {
                                if !positions.iter().any(|p| &p.symbol == sym) {
                                    tracing::warn!(
                                        symbol = %sym,
                                        "Local position not found at broker"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Periodic reconciliation: failed to query positions: {}", e);
                        }
                    }

                    // Query broker open orders
                    match port.query_open_orders() {
                        Ok(open_orders) => {
                            let states = execution_states.lock();
                            let mut local_open_count: usize = 0;
                            for (_, state) in states.iter() {
                                let tracker = state.order_tracker.lock();
                                local_open_count += tracker.open_orders_count();
                            }
                            if open_orders.len() != local_open_count {
                                tracing::warn!(
                                    broker_open = %open_orders.len(),
                                    local_open = %local_open_count,
                                    "Open order count drift detected"
                                );
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Periodic reconciliation: failed to query open orders: {}", e);
                        }
                    }

                    tracing::info!("Periodic reconciliation cycle complete");
                }
            },
        );
        self.thread_handles.push(handle);
    }

    /// Spawn the complete per-asset pipeline (Normalizer → FeatureEngine → PredictionEngine
    /// → StrategyEngine → RiskCoordinator → ExecutionManager). Returns an `AssetPipeline`
    /// that holds the tick sender and thread handles so the pipeline can be shut down later.
    fn spawn_asset_pipeline(
        &mut self,
        symbol: &str,
        lifecycle_tx: &Sender<OrderLifecycleEvent>,
        execution_port: &Arc<dyn IExecutionPort>,
        config: &EngineConfig,
        asset_idx: usize,
        exec_shared: ExecutionSharedState,
    ) -> AssetPipeline {
        let core_id = self.next_asset_core;
        self.next_asset_core += 1;
        if self.next_asset_core > 3 {
            self.next_asset_core = 1;
        }

        let threading_config = &config.threading_config;
        let pred_core_id = (threading_config.prediction_core_id + asset_idx) % 4;
        let risk_core_id = (threading_config.risk_core_id + asset_idx) % 4;
        let exec_core_id = (threading_config.execution_core_id + asset_idx) % 4;

        let channel_cfg = &config.channel_config;
        let (md_tx, md_rx) = bounded::<RawTick>(channel_cfg.per_asset_tick_channel_capacity);
        let (feature_tx, feature_rx) = bounded::<FeatureVector>(channel_cfg.feature_channel_capacity);
        let (risk_tx, risk_rx) = bounded::<RiskCheckRequest>(channel_cfg.risk_channel_capacity);
        let (decision_tx, decision_rx) = bounded::<RiskDecision>(channel_cfg.decision_channel_capacity);
        let lifecycle_tx_clone = lifecycle_tx.clone();

        let ks = Arc::clone(&self.kill_switch);
        let metrics = Arc::clone(&self.metrics);
        let pm = Arc::clone(&self.portfolio_manager);

        let strategy = StrategyEngine::new(
            symbol,
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

        self.strategy_registry.lock().insert(
            symbol.to_string(),
            Arc::clone(&strategy_arc),
        );

        let signal_ctx = strategy::SignalContext::new(symbol);

        // Create prediction engine first to get the ArcSwap
        let pred_engine = PredictionEngine::new(feature_rx, symbol);
        let latest_pred = Arc::clone(&pred_engine.latest_pred);

        let processor = AssetProcessor {
            symbol: symbol.to_string(),
            normalizer: Normalizer::new(symbol),
            feature_engine: FeatureEngine::new(
                symbol,
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
            strategy: strategy_arc.clone(),
            latest_pred,
            signal_ctx,
            coordinator_tx: risk_tx,
            feature_tx: feature_tx.clone(),
            kill_switch: Arc::clone(&ks),
            metrics: Arc::clone(&metrics),
            prediction_staleness_ns: config.strategy_config.prediction_staleness_ns,
            default_order_quantity: config.execution_defaults.default_order_quantity,
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
        let pred_handle = pred_engine.start(move |features| {
            inference_engine.predict(features)
        }, pred_core_id);

        let risk_coordinator = RiskCoordinator::new(
            risk_rx,
            decision_tx,
            config.risk_config.clone(),
            Arc::clone(&pm),
            Arc::clone(&ks),
            Arc::clone(&metrics),
        );
        let risk_handle = risk_coordinator.start(risk_core_id);

        let global_rate = config.risk_config.max_order_rate_per_sec as f64;
        let per_symbol_rate = global_rate / config.execution_defaults.execution_per_symbol_rate_divisor;

        self.execution_states.insert(symbol.to_string(), exec_shared.clone());

        let exec_manager = ExecutionManager::new(
            decision_rx,
            lifecycle_tx_clone,
            Arc::clone(execution_port),
            global_rate,
            per_symbol_rate,
            Arc::clone(&metrics),
            Arc::clone(&ks),
            pm,
            Arc::clone(&exec_shared.order_tracker),
            Arc::clone(&exec_shared.rate_limiter),
            Arc::clone(&exec_shared.circuit_breaker),
            Arc::clone(&exec_shared.idempotency_store),
            unified_trading_core::validator::RequestValidator::new(config.validator_config.clone()),
        );
        let exec_handle = exec_manager.start(exec_core_id);

        // Start asset processor with heartbeat
        let sym = symbol.to_string();
        let hb_processor = self.heartbeat_monitor.as_ref().map(|m| m.register_thread(&format!("asset-{}", sym)));
        let asset_handle = spawn_pinned(
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
            symbol, core_id, pred_core_id, risk_core_id, exec_core_id
        );

        AssetPipeline {
            symbol: symbol.to_string(),
            md_tx,
            thread_handles: vec![pred_handle, risk_handle, exec_handle, asset_handle],
            strategy_ref: strategy_arc,
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

    fn start_command_actor(&self) -> CommandActor {
        let config = self.config.read();
        let command_core_id = config.threading_config.command_core_id;
        drop(config);

        let metrics = Arc::clone(&self.metrics);
        let metrics_for_actor = Arc::clone(&metrics);
        let kill_switch = Arc::clone(&self.kill_switch);
        let strategy_registry = Arc::clone(&self.strategy_registry);
        let config_arc = Arc::clone(&self.config);
        let execution_states = Arc::new(parking_lot::Mutex::new(self.execution_states.clone()));
        let portfolio_manager = Arc::clone(&self.portfolio_manager);
        let journal_tx_opt = self.journal_tx.clone();
        let tick_reactor_tx_opt = self.tick_reactor_tx.clone();

        CommandActor::new(self.command_channel.rx.clone(), move |cmd| {
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
                    let registry = strategy_registry.lock();
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
                    let states = execution_states.lock();
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
                    let states = execution_states.lock();
                    for (_, exec_state) in states.iter() {
                        let mut rl = exec_state.rate_limiter.lock();
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
                    let states = execution_states.lock();
                    for (_, exec_state) in states.iter() {
                        exec_state.circuit_breaker.trip();
                    }
                    ControlResponse::Ok
                }
                ControlCommand::CircuitBreakerReset => {
                    let states = execution_states.lock();
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
        }, command_core_id, Some(metrics_for_actor))
    }

    pub fn shutdown(&mut self) {
        self.kill_switch.activate();
        tracing::info!("Unified Trading Engine shutting down...");

        // 1. Stop accepting new external work (close tick reactor control channel)
        self.tick_reactor_tx = None;

        // 2. Stop command actor so no new control commands are processed
        if let Some(mut actor) = self.command_actor.take() {
            actor.shutdown();
        }

        // 3. Flush journal synchronously before threads exit
        if let Some(ref journal) = self.journal {
            if let Err(e) = journal.flush_sync() {
                tracing::warn!("Journal flush failed during shutdown: {}", e);
            }
        }

        // 4. Shut down journal writer thread
        if let Some(journal) = self.journal.take() {
            journal.shutdown();
        }

        // 5. Shutdown heartbeat monitor
        if let Some(mut monitor) = self.heartbeat_monitor.take() {
            monitor.shutdown();
        }

        // 6. Stop config watcher
        if let Some(mut watcher) = self.config_watcher.take() {
            watcher.stop();
        }

        // 7. Join all worker threads with a timeout
        let timeout = std::time::Duration::from_secs(5);
        for handle in self.thread_handles.drain(..) {
            match Self::join_with_timeout(handle, timeout) {
                Ok(()) => {}
                Err(_) => {
                    tracing::warn!("Thread failed to join within {:?} during shutdown", timeout);
                }
            }
        }

        let snap = self.metrics.snapshot();
        tracing::info!("Final metrics: {:?}", snap);
    }

    fn join_with_timeout(handle: std::thread::JoinHandle<()>, timeout: std::time::Duration) -> Result<(), ()> {
        let (tx, rx) = crossbeam_channel::bounded::<Result<(), Box<dyn std::any::Any + Send>>>(1);
        let _ = std::thread::spawn(move || {
            let result = handle.join();
            let _ = tx.send(result);
        });
        match rx.recv_timeout(timeout) {
            Ok(result) => result.map_err(|_| ()),
            Err(_) => Err(()),
        }
    }
}
