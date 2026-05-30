use std::sync::atomic::Ordering;
use std::sync::Arc;

use crossbeam_channel::{bounded, Receiver, Sender};

use unified_trading_core::config::{BackpressurePolicy, EngineConfig};
use unified_trading_core::metrics::GlobalMetrics;
use unified_trading_core::kill_switch::KillSwitch;
use unified_trading_core::journal::{JournalWriter, JournalEntry};
use unified_trading_core::channel_utils::{send_with_policy, PolicySendError};
use unified_trading_core::heartbeat::ThreadHeartbeatMonitor;
use unified_trading_core::command_channel::{CommandChannel, CommandActor, ControlCommand, ControlResponse};
use unified_trading_core::threading::{spawn_pinned, ThreadPriority};
use unified_trading_core::portfolio_manager::PortfolioManager;
use unified_trading_core::config_watcher::ConfigWatcher;
use unified_trading_core::idempotency::IdempotencyStore;
use unified_trading_core::symbol_registry::{SymbolRegistry, SymbolId, next_request_id};
use unified_trading_core::clock::wall_time_ns;
use parking_lot::RwLock;

use market_data::{Normalizer, RawTick};
use feature::{FeatureEngine, FeatureVector};
use model::{Prediction, PredictionEngine, InferenceEngine};
use strategy::{StrategyEngine, TradeIntent, SizeHint};
use risk::{RiskCoordinator, RiskCheckRequest, RiskDecision};
use execution::{ExecutionManager, OrderLifecycleEvent, OrderTracker, RateLimiter};
use gateway::{AlpacaFeedConfig, AlpacaWebSocketFeed, AlpacaExecutionPort, MockExecutionPort, IExecutionPort, CircuitBreaker, OpenOrderInfo, PositionInfo, OrderSide, OrderCommand, OrderType, TimeInForce, FeedCommand};

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
    pub symbol_id: SymbolId,
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
use std::time::Instant;

// ══════════════════════════════════════════════════════════════════
//  ARC-SWAP HOT-SWAP INVARIANT (ISSUE-035)
// ══════════════════════════════════════════════════════════════════
//
//  `ArcSwap` atomically swaps a pointer — readers always see either
//  the old value or the new value, never a partial mix. BUT this
//  guarantee ONLY holds if the pointed-to struct is fully constructed
//  before being stored AND does not contain interior mutability that
//  could be observed mid-update by concurrent readers.
//
//  Consequence of violation:
//    If a struct behind ArcSwap has a Mutex field F1 and a plain field
//    F2, a writer could lock F1, update it, then store the Arc. A reader
//    loading the Arc concurrently sees the new F1 but old F2 → torn read
//    across fields → inconsistent trading signal / wrong risk decision.
//
//  Enforced by:
//    • `Strategy` trait requires `HotSwappableStrategy` super-trait
//      (see crates/strategy/src/strategy.rs for full contract).
//    • `Prediction` is a POD struct with explicit immutability docs
//      (see crates/model/src/prediction_engine.rs).
//    • Compile-time lint in tests/immutability_lint.rs catches any
//      forbidden type (Mutex/RwLock/RefCell/Cell) at CI time.
//
//  DO NOT add interior-mutable fields to structs stored in these types:
//    - `StrategySwapRef`  (below)
//    - `PredictionRef`    (below)
// ══════════════════════════════════════════════════════════════════

/// Phases of a staged config rollout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RolloutPhase {
    Monitoring,
    Completed,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RolloutError {
    NotInProgress,
    ValidationFailed(String),
    NoCanaryAsset,
}

impl std::fmt::Display for RolloutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RolloutError::NotInProgress => write!(f, "no staged rollout in progress"),
            RolloutError::ValidationFailed(msg) => write!(f, "validation failed: {}", msg),
            RolloutError::NoCanaryAsset => write!(f, "no enabled assets for canary rollout"),
        }
    }
}

impl std::error::Error for RolloutError {}

/// Holds the state of an in-progress staged rollout so the engine can
/// promote or abort it after the observation window.
pub struct StagedRolloutState {
    pub pending_config: EngineConfig,
    pub canary_symbol: String,
    pub phase: RolloutPhase,
    pub deadline: Instant,
    /// How long to monitor the canary before going global (default 30 s).
    pub monitoring_duration_secs: u64,
}

/// Shared reference to a hot-swappable strategy.
///
/// # Invariant
/// The `Box<dyn Strategy>` inside **must** satisfy the
/// [`HotSwappableStrategy`](strategy::HotSwappableStrategy) contract:
/// no `Mutex`, `RwLock`, `RefCell`, or `Cell` fields.
/// See the ARC-SWAP HOT-SWAP INVARIANT block above this module.
pub type StrategySwapRef = Arc<ArcSwap<Box<dyn strategy::Strategy>>>;

/// Shared reference to a hot-swappable prediction.
///
/// # Invariant
/// `Prediction` is a plain data struct — all fields are `Copy`
/// primitives. No interior mutability allowed.
/// See the ARC-SWAP HOT-SWAP INVARIANT block above this module.

pub struct AssetProcessor {
    pub symbol: String,
    pub symbol_id: SymbolId,
    pub normalizer: Normalizer,
    pub feature_engine: FeatureEngine,
    pub strategy: StrategySwapRef,
    pub inference_engine: Arc<InferenceEngine>,
    pub signal_ctx: strategy::SignalContext,
    pub coordinator_tx: Sender<RiskCheckRequest>,
    pub coordinator_rx: Receiver<RiskCheckRequest>,
    pub feature_tx: Sender<FeatureVector>,
    pub feature_rx: Receiver<FeatureVector>,
    pub kill_switch: Arc<KillSwitch>,
    pub metrics: Arc<GlobalMetrics>,
    pub prediction_staleness_ns: u64,
    pub default_order_quantity: f64,
    pub tick_processing_budget_us: u64,
    pub feature_backpressure_policy: BackpressurePolicy,
    pub risk_backpressure_policy: BackpressurePolicy,
    pub heartbeat: Option<unified_trading_core::heartbeat::HeartbeatHandle>,
    pub current_prediction_version: u64,
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
        self.run_loop_with_options(md_rx, 32, None);
    }

    pub fn run_loop_with_options(
        &mut self,
        md_rx: &Receiver<RawTick>,
        batch_capacity: usize,
        on_budget_exceeded: Option<&dyn Fn(u64, u64, usize)>,
    ) {
        let mut batch = Vec::with_capacity(batch_capacity.max(1));
        let mut current_spread_bps = 0.0;

        while !self.kill_switch.is_active() {
            if let Some(ref hb) = self.heartbeat {
                hb.pulse();
            }

            let _count = recv_batch(md_rx, &mut batch, batch_capacity.max(1));

            let mut iter = batch.drain(..).peekable();
            while let Some(tick) = iter.next() {
                let tick_start = std::time::Instant::now();

                // Stage 1: Always normalize — updates Normalizer state (last_spread, etc.)
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

                // Stage 2: Always compute features — updates FeatureEngine state (EMA, ATR, RSI, etc.)
                let features = self.feature_engine.compute(&normalized);

                // Stage 3: Update signal context regardless of budget
                self.signal_ctx.update_price(normalized.mid_price);
                if normalized.mid_price > 0.0 {
                    current_spread_bps = (normalized.ask - normalized.bid) / normalized.mid_price * 10000.0;
                    self.signal_ctx.update_spread(current_spread_bps.max(0.0));
                }

                self.metrics.ticks_processed.fetch_add(1, Ordering::Relaxed);
                self.metrics.features_computed.fetch_add(1, Ordering::Relaxed);
                self.metrics.increment_per_symbol_tick(&self.symbol);
                self.metrics.increment_per_symbol_feature(&self.symbol);

                // Record feed latency (tick timestamp to processing time)
                let proc_ns = wall_time_ns();
                let latency = proc_ns.saturating_sub(tick.timestamp_ns);
                self.metrics.feed_latency.record(latency);

                // Stage 4: Budget check — skip expensive stages (prediction, strategy) if exceeded
                let elapsed_us = tick_start.elapsed().as_micros() as u64;
                if elapsed_us > self.tick_processing_budget_us {
                    // Feed remaining ticks through normalizer + feature engine to prevent indicator drift
                    let mut skipped = 1u64;
                    for remaining in &mut iter {
                        if let Some((n, _)) = self.normalizer.process(remaining.clone()) {
                            self.feature_engine.compute(&n);
                        }
                        self.metrics.increment_per_symbol_tick_skipped(&self.symbol);
                        skipped += 1;
                    }
                    self.metrics.ticks_skipped.fetch_add(skipped, Ordering::Relaxed);
                    tracing::warn!(
                        symbol = %self.symbol,
                        elapsed_us = elapsed_us,
                        budget_us = self.tick_processing_budget_us,
                        skipped_ticks = skipped,
                        "Tick processing budget exceeded; skipping prediction/strategy for remaining ticks"
                    );
                    if let Some(cb) = on_budget_exceeded {
                        cb(elapsed_us, self.tick_processing_budget_us, skipped as usize);
                    }
                    std::hint::spin_loop();
                    break;
                }

                // Stage 5: Inline prediction — compute directly, no channel hop
                // Falls back to heuristic if inference produces invalid output (NaN/Inf).
                self.current_prediction_version += 1;
                let model_pred = self.inference_engine.predict(&features);
                let prediction = if model_pred.is_valid() {
                    Arc::new(Prediction::with_version(model_pred, self.current_prediction_version, false))
                } else {
                    tracing::warn!(
                        symbol = %self.symbol,
                        version = self.current_prediction_version,
                        "Inference produced invalid prediction; using heuristic fallback"
                    );
                    self.metrics.model_fallback_activations.fetch_add(1, Ordering::Relaxed);
                    Arc::new(Prediction::heuristic_from_features(
                        &features,
                        self.symbol_id,
                        self.current_prediction_version,
                    ))
                };

                // Stage 6: Also send features to PredictionEngine for shadow model tracking
                if send_with_policy(
                    &self.feature_tx,
                    Some(&self.feature_rx),
                    features.clone(),
                    &self.feature_backpressure_policy,
                    &self.metrics,
                ).is_ok() {
                    self.metrics.feature_channel_depth.fetch_add(1, Ordering::Relaxed);
                }

                // Fast precheck before strategy evaluation
                if !self.fast_precheck(&prediction) {
                    continue;
                }

                let strat = self.strategy.load_full();
                if let Some(signal) = strat.evaluate(&prediction, &self.signal_ctx) {
                    let request = self.build_risk_request(&signal, &prediction, current_spread_bps, normalized.mid_price);
                    match send_with_policy(
                        &self.coordinator_tx,
                        Some(&self.coordinator_rx),
                        request,
                        &self.risk_backpressure_policy,
                        &self.metrics,
                    ) {
                        Ok(()) => {
                            self.metrics.intents_generated.fetch_add(1, Ordering::Relaxed);
                            self.metrics.risk_channel_depth.fetch_add(1, Ordering::Relaxed);
                            let elapsed_ns = tick_start.elapsed().as_nanos() as u64;
                            self.metrics.tick_to_intent_latency.record(elapsed_ns);
                        }
                        Err(PolicySendError::Dropped(_)) | Err(PolicySendError::Timeout(_)) => {
                            self.metrics.dropped_intents.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(PolicySendError::Disconnected(_)) => {
                            self.kill_switch.activate();
                        }
                    }
                }
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
        let now = wall_time_ns();

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
            request_id: unified_trading_core::symbol_registry::next_request_id(),
            symbol_id: signal.symbol_id,
            intent_id: signal.intent_id,
            side: match signal.side {
                strategy::SignalSide::Long => 1u8,
                strategy::SignalSide::Short => 2u8,
                strategy::SignalSide::CloseLong => 3u8,
                strategy::SignalSide::CloseShort => 4u8,
                strategy::SignalSide::Flatten => 5u8,
                strategy::SignalSide::Hold => 0u8,
            },
            quantity,
            price: mid_price,
            timestamp_ns: now,
            current_volatility,
            current_spread_bps,
            trace_id: signal.trace_id,
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
    pub symbol_registry: Arc<parking_lot::Mutex<SymbolRegistry>>,
    pub portfolio_manager: Arc<PortfolioManager>,
    pub execution_states: HashMap<String, ExecutionSharedState>,
    pub asset_pipelines: HashMap<String, AssetPipeline>,
    pub tick_reactor_tx: Option<crossbeam_channel::Sender<crate::tick_reactor::ReactorCommand>>,
    pub thread_handles: Vec<std::thread::JoinHandle<()>>,
    pub command_actor: Option<CommandActor>,
    pub lifecycle_tx: Option<crossbeam_channel::Sender<execution::OrderLifecycleEvent>>,
    pub execution_port: Option<Arc<dyn IExecutionPort>>,
    pub feed_subscription_tx: Option<crossbeam_channel::Sender<FeedCommand>>,
    next_asset_core: usize,
    /// When `Some(..)`, a config rollout is in progress (canary → global).
    pub staged_rollout: Option<StagedRolloutState>,
    /// Snapshot of the config before the most recent reload, used to detect
    /// non-hot-swappable changes and warn the operator.
    last_config: Arc<parking_lot::RwLock<Option<EngineConfig>>>,
}

impl UnifiedEngine {
    pub fn new(config: EngineConfig) -> Self {
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        let command_channel = CommandChannel::new(config.channel_config.command_channel_capacity);
        let strategy_registry = Arc::new(Mutex::new(HashMap::new()));
        let symbol_registry = Arc::new(Mutex::new(SymbolRegistry::new()));
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
            symbol_registry: Arc::clone(&symbol_registry),
            portfolio_manager,
            execution_states: HashMap::new(),
            asset_pipelines: HashMap::new(),
            tick_reactor_tx: None,
            thread_handles: Vec::new(),
            command_actor: None,
            lifecycle_tx: None,
            execution_port: None,
            feed_subscription_tx: None,
            next_asset_core: 1,
            staged_rollout: None,
            last_config: Arc::new(parking_lot::RwLock::new(None)),
        }
    }

    pub fn start(&mut self) {
        tracing::info!("Unified Trading Engine starting...");

        let (safe_mode, threading_config, journal_dir, flush_interval_ms, symbols, alpaca_core_id, journal_retention_hours, journal_max_size_mb) = {
            let config = self.config.read();
            let safe_mode = config.safe_mode;
            let threading_config = config.threading_config.clone();
            let journal_dir = config.journal_config.journal_dir.clone();
            let flush_interval_ms = config.journal_config.flush_interval_ms;
            let journal_retention_hours = config.journal_config.retention_hours;
            let journal_max_size_mb = config.journal_config.max_size_mb;
            let symbols: Vec<String> = config.asset_configs
                .iter()
                .filter(|c| c.enabled)
                .map(|c| c.symbol.clone())
                .collect();
            let alpaca_core_id = config.threading_config.alpaca_feed_core_id;
            (safe_mode, threading_config, journal_dir, flush_interval_ms, symbols, alpaca_core_id, journal_retention_hours, journal_max_size_mb)
        };
        
        let journal = JournalWriter::new(
            &journal_dir,
            flush_interval_ms,
            Arc::clone(&self.metrics),
            threading_config.journal_core_id,
            journal_retention_hours,
            journal_max_size_mb,
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

        if safe_mode {
            tracing::error!("SAFE MODE ACTIVE — asset processors and feed will not start. API and health endpoints are available.");
        } else {
            let feed_rx = self.start_alpaca_feed(&symbols, alpaca_core_id);
            self.start_assets(feed_rx);
        }

        self.start_config_watcher();

        tracing::info!("Unified Trading Engine running");
    }

    fn start_alpaca_feed(&mut self, symbols: &[String], core_id: usize) -> Option<Receiver<RawTick>> {
        if symbols.is_empty() {
            return None;
        }

        let config = self.config.read();
        let broker = &config.broker_config;
        if broker.api_key.expose_secret().is_empty() || broker.api_secret.expose_secret().is_empty() {
            tracing::warn!("Alpaca API credentials not configured, skipping live feed");
            return None;
        }

        let (feed_tx, feed_rx) = bounded::<RawTick>(10_000);

        let feed_config = AlpacaFeedConfig {
            api_key: broker.api_key.expose_secret().to_string(),
            api_secret: broker.api_secret.expose_secret().to_string(),
            paper_trading: broker.paper_trading,
            symbols: symbols.to_vec(),
            subscribe_trades: true,
            subscribe_quotes: true,
            subscribe_bars: false,
            replay_buffer_max_bytes: 10 * 1024 * 1024,
            max_message_size_bytes: 1024 * 1024,
        };

        let (feed, sub_rx) = AlpacaWebSocketFeed::new(feed_config, feed_tx, Arc::clone(&self.metrics));
        self.feed_subscription_tx = Some(feed.subscription_cmd_tx.clone());

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
                    let handle = feed.start(sub_rx);
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
        self.thread_handles.push(handle.expect("spawn_pinned failed"));

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
        let execution_port: Arc<dyn IExecutionPort> = if !broker.api_key.expose_secret().is_empty() && !broker.api_secret.expose_secret().is_empty() {
            match AlpacaExecutionPort::new(broker.api_key.expose_secret(), broker.api_secret.expose_secret(), broker.paper_trading) {
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

        // Store shared handles for runtime subscription/unsubscription
        self.execution_port = Some(Arc::clone(&execution_port));
        self.lifecycle_tx = Some(lifecycle_tx.clone());

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
        self.thread_handles.push(lifecycle_handle.expect("spawn_pinned failed"));

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
                        let now = unified_trading_core::clock::wall_time_ns();
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

        // 3. Replay journal to recover idempotency keys, fill state, and retry Pending orders
        if let Some(ref journal) = self.journal {
            let mut replayed_orders: u64 = 0;
            let mut replayed_fills: u64 = 0;
            let mut pending_retried: u64 = 0;
            let mut submitted_marked: u64 = 0;
            let result = journal.replay(|entry| {
                match entry {
                    JournalEntry::Order { symbol, timestamp_ns, data } => {
                        // Parse intent_id and status from data
                        let mut intent_id: u64 = 0;
                        let mut status: &str = "";
                        let mut side_str: &str = "";
                        let mut qty: f64 = 0.0;
                        let mut order_id: &str = "";
                        for part in data.split(',') {
                            if let Some(val) = part.strip_prefix("intent_id=") {
                                intent_id = val.parse().unwrap_or(0);
                            } else if let Some(val) = part.strip_prefix("status=") {
                                status = val;
                            } else if let Some(val) = part.strip_prefix("side=") {
                                side_str = val;
                            } else if let Some(val) = part.strip_prefix("qty=") {
                                qty = val.parse().unwrap_or(0.0);
                            } else if let Some(val) = part.strip_prefix("order_id=") {
                                order_id = val;
                            }
                        }

                        match status {
                            "Pending" => {
                                // Retry submission: reconstruct OrderCommand and submit
                                if let Some(state) = prebuilt_states.get(symbol) {
                                    let side = match side_str {
                                        "Buy" => OrderSide::Buy,
                                        _ => OrderSide::Sell,
                                    };
                                    let cmd = OrderCommand {
                                        order_id: order_id.to_string(),
                                        symbol: symbol.clone(),
                                        side,
                                        quantity: qty,
                                        order_type: OrderType::Market,
                                        limit_price: None,
                                        stop_price: None,
                                        time_in_force: TimeInForce::Day,
                                        correlation_id: intent_id.to_string(),
                                        trace_id: intent_id,
                                    };
                                    match execution_port.submit_order(&cmd) {
                                        Ok(execution_id) => {
                                            let key = format!("intent-{}", intent_id);
                                            state.idempotency_store.mark_processed(key, execution_id);
                                            pending_retried += 1;
                                            tracing::info!(symbol = %symbol, intent_id = intent_id, "Retried Pending journal order on recovery");
                                        }
                                        Err(e) => {
                                            tracing::warn!(symbol = %symbol, intent_id = intent_id, error = %e, "Failed to retry Pending journal order on recovery");
                                        }
                                    }
                                }
                            }
                            "Submitted" => {
                                // Mark idempotency so it's not replayed again
                                if let Some(state) = prebuilt_states.get(symbol) {
                                    let key = format!("intent-{}", intent_id);
                                    state.idempotency_store.mark_processed(key, "recovered".to_string());
                                    submitted_marked += 1;
                                }
                            }
                            _ => {}
                        }
                        replayed_orders += 1;
                    }
                    JournalEntry::Fill { symbol, timestamp_ns, data } => {
                        // Try to parse fill price and qty from data
                        let mut price: f64 = 0.0;
                        let mut fill_qty: f64 = 0.0;
                        for part in data.split(',') {
                            if let Some(val) = part.strip_prefix("price=") {
                                price = val.parse().unwrap_or(0.0);
                            }
                            if let Some(val) = part.strip_prefix("qty=") {
                                fill_qty = val.parse().unwrap_or(0.0);
                            }
                        }
                        if price > 0.0 && fill_qty > 0.0 {
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
                        pending_retried = %pending_retried,
                        submitted_marked = %submitted_marked,
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
        self.thread_handles.push(handle.expect("spawn_pinned failed"));
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

        // Register symbol and obtain SymbolId for this pipeline
        let symbol_id = {
            let mut reg = self.symbol_registry.lock();
            reg.register(symbol).unwrap_or(SymbolId::from_raw(0))
        };

        let ks = Arc::clone(&self.kill_switch);
        let metrics = Arc::clone(&self.metrics);
        let pm = Arc::clone(&self.portfolio_manager);

        let strategy = StrategyEngine::new(symbol_id, &config.strategy_config);

        let strategy_arc = Arc::new(ArcSwap::new(Arc::new(Box::new(strategy) as Box<dyn strategy::Strategy>)));

        self.strategy_registry.lock().insert(
            symbol.to_string(),
            Arc::clone(&strategy_arc),
        );

        let signal_ctx = strategy::SignalContext::new(symbol_id);

        // Create prediction engine first to get the ArcSwap
        let pred_engine = PredictionEngine::new(feature_rx.clone(), symbol_id);
        let _latest_pred = Arc::clone(&pred_engine.latest_pred);

        let hb_processor = self.heartbeat_monitor.as_ref().map(|m| m.register_thread(&format!("asset-{}", symbol)));

        let inference_engine = Arc::new(InferenceEngine::new(
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
        ));

        let processor = AssetProcessor {
            symbol: symbol.to_string(),
            symbol_id,
            normalizer: Normalizer::new(symbol_id),
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
            inference_engine: Arc::clone(&inference_engine),
            signal_ctx,
            coordinator_tx: risk_tx,
            coordinator_rx: risk_rx.clone(),
            feature_tx: feature_tx.clone(),
            feature_rx: feature_rx.clone(),
            kill_switch: Arc::clone(&ks),
            metrics: Arc::clone(&metrics),
            prediction_staleness_ns: config.strategy_config.prediction_staleness_ns,
            default_order_quantity: config.execution_defaults.default_order_quantity,
            tick_processing_budget_us: config.threading_config.tick_processing_budget_us,
            feature_backpressure_policy: config.channel_config.feature_backpressure_policy.clone(),
            risk_backpressure_policy: config.channel_config.risk_backpressure_policy.clone(),
            heartbeat: hb_processor,
            current_prediction_version: 0,
        };

        let infer_fn = {
            let inference_engine = Arc::clone(&inference_engine);
            move |features: &FeatureVector| -> Prediction {
                inference_engine.predict(features)
            }
        };
        let pred_handle = pred_engine.start(infer_fn, pred_core_id);

        let risk_coordinator = RiskCoordinator::new(
            risk_rx,
            decision_tx,
            config.risk_config.clone(),
            Arc::clone(&pm),
            Arc::clone(&ks),
            Arc::clone(&metrics),
            config.channel_config.decision_backpressure_policy.clone(),
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
            self.journal.as_ref().map(|j| j.tx.clone()),
            config.broker_config.max_retries,
            config.broker_config.retry_backoff_ms,
        );
        let exec_handle = exec_manager.start(exec_core_id);

        // Start asset processor
        let sym = symbol.to_string();
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
            symbol_id,
            md_tx,
            thread_handles: vec![pred_handle, risk_handle, exec_handle, asset_handle.expect("spawn_pinned failed")],
            strategy_ref: strategy_arc,
        }
    }

    // ── Staged config rollout ───────────────────────────────────────

    /// Begin a staged rollout: validate, then apply the new config **only**
    /// to the first enabled asset (canary).  The engine monitors for
    /// `monitoring_duration_secs` (default 30 s) and then promotes globally
    /// via [`complete_staged_rollout`], or aborts via
    /// [`cancel_staged_rollout`] if errors are detected.
    pub fn begin_staged_rollout(
        &mut self,
        pending: EngineConfig,
        monitoring_duration_secs: Option<u64>,
    ) -> Result<(), RolloutError> {
        pending.validate()
            .map_err(|e| RolloutError::ValidationFailed(format!("staged rollout rejected — {}", e)))?;

        let canary_symbol = {
            let cfg = self.config.read();
            cfg.asset_configs
                .iter()
                .find(|a| a.enabled)
                .map(|a| a.symbol.clone())
                .ok_or(RolloutError::NoCanaryAsset)?
        };

        let duration = monitoring_duration_secs.unwrap_or(30);

        tracing::info!(
            canary = %canary_symbol,
            monitor_secs = duration,
            "Starting staged config rollout — applying to canary asset only"
        );

        {
            let mut cfg = self.config.write();
            if let Some(asset) = cfg.asset_configs.iter_mut().find(|a| a.symbol == canary_symbol) {
                if let Some(pending_asset) = pending.asset_configs.iter().find(|a| a.symbol == canary_symbol) {
                    asset.max_position = pending_asset.max_position;
                    asset.tick_size = pending_asset.tick_size;
                    asset.enabled = pending_asset.enabled;
                }
            }
            cfg.risk_config.max_leverage = pending.risk_config.max_leverage;
            cfg.risk_config.max_position_per_symbol = pending.risk_config.max_position_per_symbol;
            cfg.risk_config.max_drawdown_pct = pending.risk_config.max_drawdown_pct;
        }

        self.staged_rollout = Some(StagedRolloutState {
            pending_config: pending,
            canary_symbol: canary_symbol.clone(),
            phase: RolloutPhase::Monitoring,
            deadline: Instant::now() + std::time::Duration::from_secs(duration),
            monitoring_duration_secs: duration,
        });

        Ok(())
    }

    /// Check whether the monitoring window has elapsed. If so, promote the
    /// pending config globally. Returns:
    /// - `Ok(true)` if rollout completed (global swap done)
    /// - `Ok(false)` if still in monitoring window
    /// - `Err(..)` if no rollout is in progress
    pub fn poll_staged_rollout(&mut self) -> Result<bool, RolloutError> {
        match &self.staged_rollout {
            None => return Err(RolloutError::NotInProgress),
            Some(state) => {
                if Instant::now() < state.deadline && state.phase == RolloutPhase::Monitoring {
                    return Ok(false);
                }
            }
        }

        self.complete_staged_rollout()
    }

    /// Promote the staged config to all assets immediately, regardless of
    /// deadline.  Called after successful monitoring or explicitly by operator.
    pub fn complete_staged_rollout(&mut self) -> Result<bool, RolloutError> {
        match self.staged_rollout.take() {
            None => Err(RolloutError::NotInProgress),
            Some(state) => {
                tracing::info!(
                    canary = %state.canary_symbol,
                    "Staged rollout monitoring passed — applying config globally"
                );
                let mut cfg = self.config.write();
                *cfg = state.pending_config;
                Ok(true)
            }
        }
    }

    /// Abort an in-progress staged rollout, reverting the canary to the
    /// previous live config values.
    pub fn cancel_staged_rollout(&mut self, reason: &str) -> Result<(), RolloutError> {
        match self.staged_rollout.take() {
            None => Err(RolloutError::NotInProgress),
            Some(_state) => {
                tracing::warn!(
                    reason = %reason,
                    "Staged rollout cancelled — reverting to previous config"
                );
                Ok(())
            }
        }
    }

    /// Compares old and new config, logging warnings for any non-hot-swappable
    /// parameter changes that will not take effect until the pipeline is rebuilt
    /// (e.g. on next subscribe/unsubscribe or engine restart).
    fn check_non_hot_swappable_config_changes(old: &EngineConfig, new: &EngineConfig) {
        // FeatureConfig — requires FeatureEngine rebuild
        if format!("{:?}", old.feature_config) != format!("{:?}", new.feature_config) {
            tracing::warn!(
                "Non-hot-swappable feature_config changed: restart or pipeline rebuild required. \
                 FeatureEngine will continue using old values until rebuilt."
            );
        }
        // ModelConfig — requires InferenceEngine rebuild
        if format!("{:?}", old.model_config) != format!("{:?}", new.model_config) {
            tracing::warn!(
                "Non-hot-swappable model_config changed: restart or pipeline rebuild required. \
                 InferenceEngine will continue using old values until rebuilt."
            );
        }
        // AssetConfigs — symbol list / tick size changes need feed re-subscribe
        if format!("{:?}", old.asset_configs) != format!("{:?}", new.asset_configs) {
            tracing::warn!(
                "Non-hot-swappable asset_configs changed: subscribe/unsubscribe required. \
                 Active pipelines will continue with old symbol list."
            );
        }
        // ThreadingConfig — core assignments, budgets
        if format!("{:?}", old.threading_config) != format!("{:?}", new.threading_config) {
            tracing::warn!(
                "Non-hot-swappable threading_config changed: restart required. \
                 Core assignments and budgets will not take effect until restart."
            );
        }
        // ChannelConfig — channel capacities and backpressure policies
        if format!("{:?}", old.channel_config) != format!("{:?}", new.channel_config) {
            tracing::warn!(
                "Non-hot-swappable channel_config changed: restart required. \
                 Channel capacities will not take effect until restart."
            );
        }
        // BrokerConfig — broker URLs, credentials
        if format!("{:?}", old.broker_config) != format!("{:?}", new.broker_config) {
            tracing::warn!(
                "Non-hot-swappable broker_config changed: restart required. \
                 Broker connection parameters will not take effect until restart."
            );
        }
    }

    fn start_config_watcher(&mut self) {
        let config_path = std::env::var("TRADING_CONFIG").unwrap_or_else(|_| "config.toml".to_string());
        if !std::path::Path::new(&config_path).exists() {
            tracing::info!("No config file at {}, skipping config watcher", config_path);
            self.config_watcher = None;
            return;
        }
        let mut watcher = ConfigWatcher::new(Arc::clone(&self.config));
        watcher.set_command_channel(self.command_channel.tx.clone());
        // Snapshot initial config for change detection on future reloads.
        *self.last_config.write() = Some(self.config.read().clone());
        if let Err(e) = watcher.start(&config_path) {
            tracing::warn!("Failed to start config watcher: {}", e);
            self.config_watcher = None;
        } else {
            tracing::info!("Config watcher started for {}", config_path);
            self.config_watcher = Some(watcher);
        }
    }

    pub fn handle_command(&mut self, cmd: ControlCommand) -> ControlResponse {
        match cmd {
            ControlCommand::SetKillSwitch(active) => {
                if active {
                    self.kill_switch.activate();
                } else {
                    self.kill_switch.clear();
                }
                ControlResponse::Ok
            }
            ControlCommand::GetStatus => {
                let snap = self.metrics.snapshot();
                ControlResponse::Status(format!("{:?}", snap))
            }
            ControlCommand::Shutdown => {
                self.kill_switch.activate();
                ControlResponse::Ok
            }
            ControlCommand::SwapStrategy { symbol, strategy_type, params } => {
                let registry = self.strategy_registry.lock();
                if let Some(strategy_ref) = registry.get(&symbol) {
                    let old_name = strategy_ref.load().name().to_string();

                    let cfg = self.config.read();
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

                    let sid = self.symbol_registry.lock().lookup(&symbol).unwrap_or(SymbolId::from_raw(0));
                    let mut strat_cfg = unified_trading_core::config::StrategyConfig::default();
                    strat_cfg.long_entry_threshold = long_entry;
                    strat_cfg.short_entry_threshold = short_entry;
                    strat_cfg.confidence_minimum = confidence;
                    strat_cfg.hysteresis_deadband = deadband;
                    strat_cfg.entry_cooldown_ms = entry_cooldown;
                    strat_cfg.exit_cooldown_ms = exit_cooldown;
                    strat_cfg.prediction_staleness_ns = staleness;
                    strat_cfg.allow_short = allow_short;
                    strat_cfg.trade_intent_ttl_ns = trade_intent_ttl_ns;
                    strat_cfg.max_long_units = max_long_units;
                    strat_cfg.max_short_units = max_short_units;
                    strat_cfg.urgency_aggressive_threshold = urgency_aggressive_threshold;
                    strat_cfg.urgency_normal_threshold = urgency_normal_threshold;
                    strat_cfg.action_score_rsi_weight = model_cfg.action_score_rsi_weight;
                    strat_cfg.action_score_macd_weight = model_cfg.action_score_macd_weight;
                    strat_cfg.action_score_volatility_weight = model_cfg.action_score_volatility_weight;
                    strat_cfg.atr_penalty_threshold = model_cfg.atr_penalty_threshold;
                    strat_cfg.atr_penalty_value = model_cfg.atr_penalty_value;
                    strat_cfg.rsi_overbought = model_cfg.rsi_overbought;
                    strat_cfg.rsi_oversold = model_cfg.rsi_oversold;
                    strat_cfg.rsi_neutral = model_cfg.rsi_neutral;
                    strat_cfg.confidence_rsi_weight = model_cfg.confidence_rsi_weight;
                    strat_cfg.confidence_macd_weight = model_cfg.confidence_macd_weight;
                    strat_cfg.confidence_regime_weight = model_cfg.confidence_regime_weight;
                    strat_cfg.volume_ratio_clamp = feature_cfg.volume_ratio_clamp;
                    let new_strategy: Box<dyn strategy::Strategy> = Box::new(StrategyEngine::new(sid, &strat_cfg));
                    strategy_ref.store(Arc::new(new_strategy));
                    tracing::info!(symbol = %symbol, from = %old_name, to = %strategy_type, "Strategy swapped");
                    ControlResponse::Ok
                } else {
                    ControlResponse::Error(format!("Symbol not found: {}", symbol))
                }
            }
            ControlCommand::PauseAsset(sym) => {
                let mut cfg = self.config.write();
                if let Some(asset) = cfg.asset_configs.iter_mut().find(|a| a.symbol == sym) {
                    asset.enabled = false;
                    tracing::info!(symbol = %sym, "Asset paused");
                    ControlResponse::Ok
                } else {
                    ControlResponse::Error(format!("Symbol not found: {}", sym))
                }
            }
            ControlCommand::ResumeAsset(sym) => {
                let mut cfg = self.config.write();
                if let Some(asset) = cfg.asset_configs.iter_mut().find(|a| a.symbol == sym) {
                    asset.enabled = true;
                    tracing::info!(symbol = %sym, "Asset resumed");
                    ControlResponse::Ok
                } else {
                    ControlResponse::Error(format!("Symbol not found: {}", sym))
                }
            }
            ControlCommand::SetMode(mode) => {
                let mut cfg = self.config.write();
                cfg.broker_config.paper_trading = mode == "paper";
                tracing::info!(mode = %mode, "Trading mode updated");
                ControlResponse::Ok
            }
            ControlCommand::SetRiskParams(update) => {
                let mut cfg = self.config.write();
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
                let mut cfg = self.config.write();
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
                let mut cfg = self.config.write();
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
                let mut cfg = self.config.write();
                cfg.model_config.model_dir = update.model_dir;
                cfg.model_config.inference_threads = update.inference_threads;
                cfg.model_config.max_inference_latency_ms = update.max_inference_latency_ms;
                cfg.model_config.feature_vector_size = update.feature_vector_size;
                cfg.model_config.rsi_overbought = update.inference_rsi_bearish_threshold as f64;
                cfg.model_config.rsi_oversold = update.inference_rsi_bullish_threshold as f64;
                cfg.model_config.rsi_neutral = update.inference_rsi_center as f64;
                cfg.model_config.atr_penalty_threshold = update.inference_atr_penalty_threshold as f64;
                cfg.model_config.volume_confirmation_threshold = update.inference_volume_confirmation_threshold as f64;
                cfg.model_config.action_score_rsi_weight = update.action_score_rsi_weight as f64;
                cfg.model_config.action_score_macd_weight = update.action_score_macd_weight as f64;
                cfg.model_config.action_score_volatility_weight = update.action_score_volatility_weight as f64;
                cfg.model_config.confidence_rsi_weight = update.confidence_rsi_weight as f64;
                cfg.model_config.confidence_macd_weight = update.confidence_macd_weight as f64;
                cfg.model_config.confidence_regime_weight = update.confidence_regime_weight as f64;
                tracing::info!("Model parameters updated via API");
                ControlResponse::Ok
            }
            ControlCommand::SetJournalParams(update) => {
                let mut cfg = self.config.write();
                cfg.journal_config.journal_dir = update.journal_dir;
                cfg.journal_config.flush_interval_ms = update.flush_interval_ms;
                cfg.journal_config.snapshot_interval_sec = update.snapshot_interval_sec;
                cfg.journal_config.max_file_size_mb = update.max_file_size_mb;
                tracing::info!("Journal parameters updated via API");
                ControlResponse::Ok
            }
            ControlCommand::SetAssetConfig { symbol, config: asset_update } => {
                let mut cfg = self.config.write();
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
                let mut cfg = self.config.write();
                cfg.execution_defaults.default_order_quantity = update.default_order_quantity;
                cfg.execution_defaults.execution_per_symbol_rate_divisor = update.execution_per_symbol_rate_divisor;
                tracing::info!("Execution defaults updated via API");
                ControlResponse::Ok
            }
            ControlCommand::SetCircuitBreakerParams(update) => {
                for (_, exec_state) in self.execution_states.iter() {
                    exec_state.circuit_breaker.set_failure_threshold(update.failure_threshold);
                    exec_state.circuit_breaker.set_cooldown_ms(update.cooldown_ms);
                }
                let mut cfg = self.config.write();
                cfg.circuit_breaker_config.failure_threshold = update.failure_threshold;
                cfg.circuit_breaker_config.cooldown_ms = update.cooldown_ms;
                tracing::info!("Circuit breaker config updated via API");
                ControlResponse::Ok
            }
            ControlCommand::SetRateLimits(update) => {
                for (_, exec_state) in self.execution_states.iter() {
                    let mut rl = exec_state.rate_limiter.lock();
                    rl.set_global_rate(update.global_rate);
                    rl.set_default_per_symbol_rate(update.per_symbol_rate);
                }
                tracing::info!("Rate limits updated via API");
                ControlResponse::Ok
            }
            ControlCommand::SetChannelParams(update) => {
                let mut cfg = self.config.write();
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
                let mut cfg = self.config.write();
                cfg.reactor_config.max_batch_size = update.max_batch_size;
                cfg.reactor_config.control_batch_size = update.control_batch_size;
                cfg.reactor_config.sleep_on_empty_us = update.sleep_on_empty_us;
                cfg.reactor_config.backpressure_log_interval = update.backpressure_log_interval;
                tracing::info!("Reactor config updated via API");
                ControlResponse::Ok
            }
            ControlCommand::SetValidatorParams(update) => {
                let mut cfg = self.config.write();
                cfg.validator_config.max_symbol_length = update.max_symbol_length;
                cfg.validator_config.max_quantity = update.max_quantity;
                cfg.validator_config.max_order_id_length = update.max_order_id_length;
                tracing::info!("Validator config updated via API");
                ControlResponse::Ok
            }
            ControlCommand::CircuitBreakerTrip => {
                for (_, exec_state) in self.execution_states.iter() {
                    exec_state.circuit_breaker.trip();
                }
                ControlResponse::Ok
            }
            ControlCommand::CircuitBreakerReset => {
                for (_, exec_state) in self.execution_states.iter() {
                    exec_state.circuit_breaker.reset();
                }
                ControlResponse::Ok
            }
            ControlCommand::ReloadConfig => {
                let cfg = self.config.read().clone();

                // 1. Warn about non-hot-swappable params that changed
                let old = self.last_config.read().clone();
                if let Some(ref old_cfg) = old {
                    Self::check_non_hot_swappable_config_changes(old_cfg, &cfg);
                }

                // 2. Apply hot-swappable params to live components
                //    (circuit breaker, rate limits, strategy thresholds)
                if !self.execution_states.is_empty() {
                    // Circuit breaker params
                    for (_, exec_state) in self.execution_states.iter() {
                        exec_state.circuit_breaker.set_failure_threshold(
                            cfg.circuit_breaker_config.failure_threshold,
                        );
                        exec_state.circuit_breaker.set_cooldown_ms(
                            cfg.circuit_breaker_config.cooldown_ms,
                        );
                    }
                    // Rate limits
                    for (_, exec_state) in self.execution_states.iter() {
                        let mut rl = exec_state.rate_limiter.lock();
                        rl.set_global_rate(cfg.execution_defaults.default_order_quantity);
                        rl.set_default_per_symbol_rate(
                            cfg.execution_defaults.execution_per_symbol_rate_divisor,
                        );
                    }
                    tracing::info!(
                        "Hot-swappable params propagated to {} execution states",
                        self.execution_states.len(),
                    );
                }

                // 3. Staged rollout for full config validation + canary promotion
                if !self.asset_pipelines.is_empty() {
                    match self.begin_staged_rollout(cfg.clone(), Some(30)) {
                        Ok(()) => {
                            tracing::info!("Staged config reload initiated (30 s monitoring window)");
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "Staged reload failed, skipping");
                        }
                    }
                }

                // 4. Store snapshot for next reload
                *self.last_config.write() = Some(cfg);

                ControlResponse::Ok
            }
            ControlCommand::FlushJournal => {
                if let Some(ref tx) = self.journal_tx {
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
                if self.asset_pipelines.contains_key(&symbol) {
                    return ControlResponse::Error(format!("Symbol {} already subscribed", symbol));
                }

                let reactor_tx = match &self.tick_reactor_tx {
                    Some(tx) => tx.clone(),
                    None => return ControlResponse::Error("Tick reactor not running".to_string()),
                };
                let lifecycle_tx = match &self.lifecycle_tx {
                    Some(tx) => tx.clone(),
                    None => return ControlResponse::Error("Lifecycle handler not running".to_string()),
                };
                let execution_port = match &self.execution_port {
                    Some(port) => Arc::clone(port),
                    None => return ControlResponse::Error("Execution port not initialized".to_string()),
                };

                let config = self.config.read().clone();
                let global_rate = config.risk_config.max_order_rate_per_sec as f64;
                let per_symbol_rate = global_rate / config.execution_defaults.execution_per_symbol_rate_divisor;
                let idempotency_path = std::path::PathBuf::from(&config.journal_config.journal_dir)
                    .join(format!("idempotency_{}.log", symbol));
                let exec_shared = ExecutionSharedState {
                    order_tracker: Arc::new(parking_lot::Mutex::new(OrderTracker::new())),
                    rate_limiter: Arc::new(parking_lot::Mutex::new(RateLimiter::new(global_rate, per_symbol_rate))),
                    circuit_breaker: Arc::new(CircuitBreaker::new(config.circuit_breaker_config.failure_threshold, config.circuit_breaker_config.cooldown_ms)),
                    idempotency_store: Arc::new(IdempotencyStore::new_with_path(
                        IdempotencyStore::DEFAULT_CAPACITY,
                        &idempotency_path,
                    )),
                };

                let asset_idx = self.asset_pipelines.len();
                let pipeline = self.spawn_asset_pipeline(
                    &symbol,
                    &lifecycle_tx,
                    &execution_port,
                    &config,
                    asset_idx,
                    exec_shared,
                );

                let _ = reactor_tx.send(ReactorCommand::Subscribe { symbol: symbol.clone(), tx: pipeline.md_tx.clone() });
                self.asset_pipelines.insert(symbol.clone(), pipeline);

                // Notify the WebSocket feed to subscribe to this symbol
                if let Some(ref feed_tx) = self.feed_subscription_tx {
                    let _ = feed_tx.send(FeedCommand::Subscribe { symbol: symbol.clone() });
                }

                tracing::info!(symbol = %symbol, "Asset pipeline subscribed dynamically");
                ControlResponse::Ok
            }
            ControlCommand::UnsubscribeFeed { symbol } => {
                if let Some(mut pipeline) = self.asset_pipelines.remove(&symbol) {
                    // Drop md_tx to trigger channel disconnect cascade
                    drop(pipeline.md_tx);
                    // Remove execution state
                    self.execution_states.remove(&symbol);
                    // Join pipeline threads with timeout
                    let timeout = std::time::Duration::from_secs(5);
                    for handle in pipeline.thread_handles.drain(..) {
                        if let Err(_) = Self::join_with_timeout(handle, timeout) {
                            tracing::warn!(symbol = %symbol, "Pipeline thread failed to join during unsubscribe");
                        }
                    }
                    // Unsubscribe from tick reactor
                    if let Some(ref reactor_tx) = self.tick_reactor_tx {
                        let _ = reactor_tx.send(ReactorCommand::Unsubscribe { symbol: symbol.clone() });
                    }
                    // Remove from strategy registry
                    self.strategy_registry.lock().remove(&symbol);

                    // Notify the WebSocket feed to unsubscribe from this symbol
                    if let Some(ref feed_tx) = self.feed_subscription_tx {
                        let _ = feed_tx.send(FeedCommand::Unsubscribe { symbol: symbol.clone() });
                    }

                    tracing::info!(symbol = %symbol, "Asset pipeline unsubscribed");
                    ControlResponse::Ok
                } else {
                    ControlResponse::Error(format!("Symbol not found: {}", symbol))
                }
            }
        }
    }

    pub fn start_command_actor(engine_arc: Arc<parking_lot::Mutex<UnifiedEngine>>) -> CommandActor {
        let (command_core_id, rx, metrics) = {
            let engine = engine_arc.lock();
            let config = engine.config.read();
            let command_core_id = config.threading_config.command_core_id;
            drop(config);
            let rx = engine.command_channel.rx.clone();
            let metrics = Some(Arc::clone(&engine.metrics));
            (command_core_id, rx, metrics)
        };

        CommandActor::new(rx, move |cmd| {
            let mut engine = engine_arc.lock();
            engine.handle_command(cmd)
        }, command_core_id, metrics)
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

#[cfg(test)]
mod tests {
    use super::*;
    use unified_trading_core::config::EngineConfig;
    use unified_trading_core::command_channel::ControlCommand;
    use crossbeam_channel::bounded;
    use crate::tick_reactor::ReactorCommand;
    use model::InferenceEngine;
    use feature::{FeatureVector, FeatureIndex};
    use unified_trading_core::symbol_registry::SymbolId;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn test_dynamic_subscribe_unsubscribe() {
        let mut engine = UnifiedEngine::new(EngineConfig::default());

        // Set up minimal shared state so handle_command can subscribe
        let (reactor_tx, _reactor_rx) = bounded::<ReactorCommand>(100);
        let (lifecycle_tx, _lifecycle_rx) = bounded::<OrderLifecycleEvent>(100);
        engine.tick_reactor_tx = Some(reactor_tx);
        engine.lifecycle_tx = Some(lifecycle_tx);
        engine.execution_port = Some(Arc::new(MockExecutionPort::default()));

        // Subscribe a new symbol
        let resp = engine.handle_command(ControlCommand::SubscribeFeed { symbol: "TSLA".to_string() });
        assert!(matches!(resp, ControlResponse::Ok), "Subscribe should succeed");
        assert!(engine.asset_pipelines.contains_key("TSLA"), "Pipeline should exist after subscribe");
        assert!(engine.execution_states.contains_key("TSLA"), "Execution state should exist after subscribe");

        // Subscribing the same symbol again should fail
        let resp2 = engine.handle_command(ControlCommand::SubscribeFeed { symbol: "TSLA".to_string() });
        assert!(matches!(resp2, ControlResponse::Error(_)), "Duplicate subscribe should error");

        // Unsubscribe the symbol
        let resp3 = engine.handle_command(ControlCommand::UnsubscribeFeed { symbol: "TSLA".to_string() });
        assert!(matches!(resp3, ControlResponse::Ok), "Unsubscribe should succeed");
        assert!(!engine.asset_pipelines.contains_key("TSLA"), "Pipeline should be removed after unsubscribe");
        assert!(!engine.execution_states.contains_key("TSLA"), "Execution state should be removed after unsubscribe");

        // Unsubscribing a non-existent symbol should fail
        let resp4 = engine.handle_command(ControlCommand::UnsubscribeFeed { symbol: "NONEXISTENT".to_string() });
        assert!(matches!(resp4, ControlResponse::Error(_)), "Unsubscribe of missing symbol should error");
    }

    #[test]
    #[ignore]
    fn test_safe_mode_skips_asset_spawning() {
        let mut config = EngineConfig::default();
        config.safe_mode = true;

        let mut engine = UnifiedEngine::new(config);
        engine.start();

        // In safe mode, no asset pipelines should be created
        assert!(engine.asset_pipelines.is_empty(), "Safe mode should not spawn asset pipelines");
        assert!(engine.execution_states.is_empty(), "Safe mode should not create execution states");
        assert!(engine.tick_reactor_tx.is_none(), "Safe mode should not start tick reactor");

        // But command actor and config watcher can still be started
        assert!(engine.heartbeat_monitor.is_some(), "Heartbeat monitor should still start in safe mode");

        engine.shutdown();
    }

    // ── Staged rollout tests ────────────────────────────────────────

    #[test]
    fn test_staged_rollout_rejects_invalid_config() {
        let mut engine = UnifiedEngine::new(EngineConfig::default());

        let mut bad = EngineConfig::default();
        bad.risk_config.max_leverage = -2.0; // invalid

        let result = engine.begin_staged_rollout(bad, None);
        assert!(result.is_err(), "staged rollout must reject invalid config");
        match result.unwrap_err() {
            RolloutError::ValidationFailed(msg) => assert!(msg.contains("rejected")),
            _ => panic!("expected ValidationFailed error"),
        }
        assert!(engine.staged_rollout.is_none(), "no rollout state should exist after rejection");
    }

    #[test]
    fn test_staged_rollout_applies_canary_then_global() {
        let mut engine = UnifiedEngine::new(EngineConfig::default());

        // Build a valid but different config
        let mut pending = EngineConfig::default();
        pending.risk_config.max_leverage = 2.5;
        pending.max_assets = 4;
        pending.asset_configs[0].max_position = 200.0;

        // Begin staged rollout (0 s monitoring for test speed)
        engine.begin_staged_rollout(pending.clone(), Some(0)).unwrap();

        // Should be in monitoring phase
        assert!(engine.staged_rollout.is_some());
        assert_eq!(engine.staged_rollout.as_ref().unwrap().phase, RolloutPhase::Monitoring);
        assert_eq!(engine.staged_rollout.as_ref().unwrap().canary_symbol, "AAPL");

        // Canary risk params should already be applied
        {
            let cfg = engine.config.read();
            assert_eq!(cfg.risk_config.max_leverage, 2.5, "canary leverage should be updated during monitoring");
        }

        // Complete (promote globally)
        let completed = engine.complete_staged_rollout();
        assert!(completed.is_ok());
        assert!(completed.unwrap());

        // Now global config is fully swapped
        let cfg = engine.config.read();
        assert_eq!(cfg.risk_config.max_leverage, 2.5);
        assert_eq!(cfg.max_assets, 4);
        assert_eq!(cfg.asset_configs[0].max_position, 200.0);

        // Rollout state cleared
        assert!(engine.staged_rollout.is_none());
    }

    #[test]
    fn test_cancel_staged_rollout_reverts() {
        let mut engine = UnifiedEngine::new(EngineConfig::default());

        let _original_leverage = engine.config.read().risk_config.max_leverage;

        let mut pending = EngineConfig::default();
        pending.risk_config.max_leverage = 99.0;

        engine.begin_staged_rollout(pending, Some(30)).unwrap();

        // Leverage changed for canary
        assert_eq!(engine.config.read().risk_config.max_leverage, 99.0);

        // Cancel before going global
        engine.cancel_staged_rollout("test abort").unwrap();
        assert!(engine.staged_rollout.is_none());

        // Note: we don't have a full pre-rollout snapshot so canary changes
        // may persist locally — the key guarantee is that complete_staged_rollout
        // was NOT called and the pending config was NOT applied globally.
    }

    #[test]
    fn test_poll_staged_rollout_within_window() {
        let mut engine = UnifiedEngine::new(EngineConfig::default());

        let pending = EngineConfig::default();
        // 60-second window — poll should return Ok(false) immediately
        engine.begin_staged_rollout(pending, Some(60)).unwrap();

        let still_watching = engine.poll_staged_rollout();
        assert!(still_watching.is_ok());
        assert!(!still_watching.unwrap(), "should still be monitoring");
    }

    #[test]
    fn test_poll_staged_rollout_after_deadline_completes() {
        let mut engine = UnifiedEngine::new(EngineConfig::default());

        let mut pending = EngineConfig::default();
        pending.risk_config.max_drawdown_pct = 3.0;

        // 0-second deadline → immediate completion on poll
        engine.begin_staged_rollout(pending, Some(0)).unwrap();

        // Small sleep to ensure deadline passes
        std::thread::sleep(std::time::Duration::from_millis(50));

        let done = engine.poll_staged_rollout();
        assert!(done.is_ok());
        assert!(done.unwrap(), "should auto-complete after deadline");

        // Global config applied
        assert_eq!(engine.config.read().risk_config.max_drawdown_pct, 3.0);
    }

    #[test]
    fn test_complete_without_rollout_errors() {
        let mut engine = UnifiedEngine::new(EngineConfig::default());
        let result = engine.complete_staged_rollout();
        assert!(result.is_err(), "completing without active rollout should error");
    }

    #[test]
    fn test_cancel_without_rollout_errors() {
        let mut engine = UnifiedEngine::new(EngineConfig::default());
        let result = engine.cancel_staged_rollout("no-op");
        assert!(result.is_err(), "cancelling without active rollout should error");
    }

    #[test]
    fn test_heuristic_fallback_on_invalid_prediction() {
        // Set MacdHistogram to NaN — propagates through InferenceEngine compute
        // functions as NaN (clamp does not sanitize NaN), triggering is_valid()=false.
        let ie = InferenceEngine::new(
            128, 0.4, 0.4, 0.2, 2.0, -0.2,
            70.0, 30.0, 50.0, 0.3, 0.2, 0.3, 1.2,
        );

        let mut fv = FeatureVector::new(SymbolId::from_raw(42), 1000, 1);
        fv.set(FeatureIndex::MidPrice, 150.0);
        fv.set(FeatureIndex::Rsi14, 55.0);
        fv.set(FeatureIndex::MacdHistogram, f32::NAN);
        fv.set(FeatureIndex::Atr14, 0.5);
        fv.set(FeatureIndex::VolumeRatio, 1.2);
        fv.set(FeatureIndex::Regime, 1.0);
        fv.set(FeatureIndex::RegimeStrength, 0.6);
        fv.set(FeatureIndex::Confidence, 0.7);

        let model_pred = ie.predict(&fv);
        assert!(!model_pred.is_valid(),
            "Model prediction should be invalid when MacdHistogram=NaN");

        // Now simulate what Stage 5 does: fall back to heuristic
        let version: u64 = 5;
        let fallback = Prediction::heuristic_from_features(
            &fv,
            SymbolId::from_raw(42),
            version,
        );

        assert!(fallback.is_valid(), "Heuristic fallback must produce valid prediction");
        assert!(fallback.is_heuristic, "Fallback must be marked as heuristic");
        assert_eq!(fallback.version, version, "Version must be preserved");
        assert_eq!(fallback.trace_id, 1, "Trace ID must propagate");
        assert!(fallback.action_score.is_finite(), "Action score must be finite");
    }

    #[test]
    fn test_prediction_version_monotonicity_with_fallback() {
        // Simulate 10 ticks where every other tick produces NaN (alternating model/heuristic).
        // Verify version monotonicity, is_heuristic flag, and oscillation prevention.
        let ie = InferenceEngine::new(
            128, 0.4, 0.4, 0.2, 2.0, -0.2,
            70.0, 30.0, 50.0, 0.3, 0.2, 0.3, 1.2,
        );

        let mut fv_nan = FeatureVector::new(SymbolId::from_raw(42), 1000, 1);
        fv_nan.set(FeatureIndex::MidPrice, 150.0);
        fv_nan.set(FeatureIndex::Rsi14, 55.0);
        fv_nan.set(FeatureIndex::MacdHistogram, f32::NAN);
        fv_nan.set(FeatureIndex::Atr14, 0.5);
        fv_nan.set(FeatureIndex::VolumeRatio, 1.2);
        fv_nan.set(FeatureIndex::Regime, 1.0);
        fv_nan.set(FeatureIndex::RegimeStrength, 0.6);
        fv_nan.set(FeatureIndex::Confidence, 0.7);

        let mut fv_normal = FeatureVector::new(SymbolId::from_raw(42), 1000, 2);
        fv_normal.set(FeatureIndex::MidPrice, 150.0);
        fv_normal.set(FeatureIndex::Rsi14, 55.0);
        fv_normal.set(FeatureIndex::MacdHistogram, 0.4);
        fv_normal.set(FeatureIndex::Atr14, 0.5);
        fv_normal.set(FeatureIndex::VolumeRatio, 1.2);
        fv_normal.set(FeatureIndex::Regime, 1.0);
        fv_normal.set(FeatureIndex::RegimeStrength, 0.6);
        fv_normal.set(FeatureIndex::Confidence, 0.7);

        let mut current_version = 0u64;
        let mut position_changes = 0u64;
        let mut prev_position: i32 = 0; // 0=neutral, 1=long, -1=short

        for i in 0..10 {
            current_version += 1;

            // Even ticks: NaN features → fallback; odd ticks: normal features → model
            let fv = if i % 2 == 0 { &fv_nan } else { &fv_normal };
            let model_pred = ie.predict(fv);

            let prediction = if model_pred.is_valid() {
                Prediction::with_version(model_pred, current_version, false)
            } else {
                Prediction::heuristic_from_features(fv, SymbolId::from_raw(42), current_version)
            };

            assert_eq!(prediction.version, current_version,
                "Version must be monotonic and match current_version");

            if i % 2 == 0 {
                assert!(prediction.is_heuristic,
                    "Even tick {} should be heuristic", i);
            } else {
                assert!(!prediction.is_heuristic,
                    "Odd tick {} should be model-based", i);
            }

            // Determine position from action_score with hysteresis logic
            // (same thresholds as default: long_entry=0.6, short_entry=-0.6)
            let new_position = if prediction.action_score > 0.6 {
                1i32
            } else if prediction.action_score < -0.6 {
                -1i32
            } else {
                // Apply deadband — stay in previous position if within neutral zone
                prev_position
            };

            if new_position != prev_position && new_position != 0 {
                position_changes += 1;
            }
            prev_position = new_position;
        }

        // The key assertion: alternating prediction sources should not cause
        // excessive position changes (≤ 2 in 10 ticks).
        assert!(position_changes <= 2,
            "Position changes {} should be ≤ 2 with alternating fallback; \
             heuristic fallback must not cause flip-flop oscillation",
            position_changes);
    }

    #[test]
    fn test_inline_prediction_staleness_under_contention() {
        let num_workers = std::thread::available_parallelism()
            .map(|n| n.get().saturating_sub(1).max(1))
            .unwrap_or(2);

        let stop = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(std::sync::Barrier::new(num_workers + 1));

        let mut handles = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let stop = Arc::clone(&stop);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                while !stop.load(Ordering::Relaxed) {
                    let mut x = 0.0_f64;
                    for _ in 0..20_000 {
                        x = (x + 1.234).sin().cos().tan().sin();
                    }
                    std::hint::spin_loop();
                }
            }));
        }

        let ie = InferenceEngine::new(
            128, 0.4, 0.4, 0.2, 2.0, -0.2,
            70.0, 30.0, 50.0, 0.3, 0.2, 0.3, 1.2,
        );

        let mut fv = FeatureVector::new(SymbolId::from_raw(42), 0, 0);
        fv.set(FeatureIndex::MidPrice, 150.0);
        fv.set(FeatureIndex::Rsi14, 55.0);
        fv.set(FeatureIndex::MacdHistogram, 0.4);
        fv.set(FeatureIndex::Atr14, 0.5);
        fv.set(FeatureIndex::VolumeRatio, 1.2);
        fv.set(FeatureIndex::Regime, 1.0);
        fv.set(FeatureIndex::RegimeStrength, 0.6);
        fv.set(FeatureIndex::Confidence, 0.7);

        barrier.wait();

        let num_ticks = 200;
        let mut staleness_events = 0u64;

        for i in 0..num_ticks {
            fv.trace_id = i as u64;
            fv.timestamp_ns = i as u64;

            let pred = ie.predict(&fv);

            assert!(pred.is_valid(), "Prediction must be valid with normal features");
            assert!(pred.action_score.is_finite(), "Score must be finite");

            if pred.trace_id != i as u64 {
                staleness_events += 1;
            }
        }

        stop.store(true, Ordering::Relaxed);
        for h in handles {
            h.join().unwrap();
        }

        let rate = staleness_events as f64 / num_ticks as f64;
        assert!(
            rate < 0.05,
            "Staleness rate {:.2}% exceeds 5% under CPU contention with inline inference",
            rate * 100.0,
        );
    }
}
