use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};

use unified_trading_core::kill_switch::KillSwitch;
use unified_trading_core::metrics::GlobalMetrics;
use unified_trading_core::symbol_registry::{SymbolId, SymbolRegistry, SymbolIdArray, next_trace_id};
use unified_trading_core::threading::{spawn_pinned, ThreadPriority};

use market_data::RawTick;

#[derive(Debug, Clone)]
pub enum ReactorCommand {
    Subscribe { symbol: String, tx: Sender<RawTick> },
    Unsubscribe { symbol: String },
    Shutdown,
}

struct SymbolHandler {
    tx: Sender<RawTick>,
    tick_count: u64,
    last_tick_ns: u64,
    dropped_count: u64,
}

pub struct TickReactor {
    tick_rx: Receiver<RawTick>,
    control_rx: Receiver<ReactorCommand>,
    control_tx: Sender<ReactorCommand>,
    handlers: HashMap<SymbolId, SymbolHandler>,
    registry: SymbolRegistry,
    handler_array: SymbolIdArray<Sender<RawTick>>,
    kill_switch: Arc<KillSwitch>,
    metrics: Arc<GlobalMetrics>,
    running: Arc<AtomicBool>,
    total_ticks: Arc<AtomicU64>,
    total_dropped: Arc<AtomicU64>,
    max_batch_size: usize,
    control_batch_size: usize,
    sleep_on_empty_us: u64,
    backpressure_log_interval: u64,
}

impl TickReactor {
    pub fn new(
        tick_rx: Receiver<RawTick>,
        kill_switch: Arc<KillSwitch>,
        metrics: Arc<GlobalMetrics>,
        max_batch_size: usize,
        control_batch_size: usize,
        sleep_on_empty_us: u64,
        backpressure_log_interval: u64,
    ) -> (Self, Sender<ReactorCommand>) {
        let (control_tx, control_rx) = bounded::<ReactorCommand>(256);

        let reactor = Self {
            tick_rx,
            control_rx,
            control_tx: control_tx.clone(),
            handlers: HashMap::new(),
            registry: SymbolRegistry::new(),
            handler_array: SymbolIdArray::new(),
            kill_switch,
            metrics,
            running: Arc::new(AtomicBool::new(true)),
            total_ticks: Arc::new(AtomicU64::new(0)),
            total_dropped: Arc::new(AtomicU64::new(0)),
            max_batch_size,
            control_batch_size,
            sleep_on_empty_us,
            backpressure_log_interval,
        };

        (reactor, control_tx)
    }

    pub fn subscribe(&mut self, symbol: String, tx: Sender<RawTick>) {
        if let Some(existing_id) = self.registry.lookup(&symbol) {
            tracing::warn!("Symbol {} already subscribed, replacing handler", symbol);
            self.handlers.remove(&existing_id);
        }

        if let Some(id) = self.registry.register(&symbol) {
            self.handler_array.set(id, tx.clone());
            self.handlers.insert(
                id,
                SymbolHandler {
                    tx,
                    tick_count: 0,
                    last_tick_ns: 0,
                    dropped_count: 0,
                },
            );
            tracing::info!("Subscribed to symbol {} (ID: {:?})", symbol, id);
        } else {
            tracing::error!("Failed to register symbol {} - registry full", symbol);
        }
    }

    pub fn unsubscribe(&mut self, symbol: &str) {
        if let Some(id) = self.registry.lookup(symbol) {
            self.handlers.remove(&id);
            tracing::info!("Unsubscribed from symbol {}", symbol);
        }
    }

    pub fn run(&mut self) {
        tracing::info!("Tick reactor started with {} symbols", self.handlers.len());

        while self.running.load(Ordering::Relaxed) && !self.kill_switch.is_active() {
            self.process_control_batch();
            self.process_tick_batch();
        }

        tracing::info!(
            "Tick reactor stopped. Total ticks: {}, dropped: {}",
            self.total_ticks.load(Ordering::Relaxed),
            self.total_dropped.load(Ordering::Relaxed),
        );
    }

    fn process_control_batch(&mut self) {
        for _ in 0..self.control_batch_size {
            match self.control_rx.try_recv() {
                Ok(ReactorCommand::Subscribe { symbol, tx }) => {
                    self.subscribe(symbol, tx);
                }
                Ok(ReactorCommand::Unsubscribe { symbol }) => {
                    self.unsubscribe(&symbol);
                }
                Ok(ReactorCommand::Shutdown) => {
                    self.running.store(false, Ordering::SeqCst);
                    return;
                }
                Err(_) => break,
            }
        }
    }

    fn process_tick_batch(&mut self) {
        let mut batch = Vec::with_capacity(self.max_batch_size);

        for _ in 0..self.max_batch_size {
            match self.tick_rx.try_recv() {
                Ok(tick) => batch.push(tick),
                Err(_) => break,
            }
        }

        if batch.is_empty() {
            std::thread::sleep(std::time::Duration::from_micros(self.sleep_on_empty_us));
            return;
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        for tick in batch.drain(..) {
            self.total_ticks.fetch_add(1, Ordering::Relaxed);
            let symbol_id = tick.symbol_id;
            let trace_id = next_trace_id();

            if let Some(tx) = self.handler_array.get(symbol_id) {
                let mut tick_with_trace = tick;
                tick_with_trace.trace_id = trace_id;
                match tx.try_send(tick_with_trace) {
                    Ok(()) => {
                        if let Some(handler) = self.handlers.get_mut(&symbol_id) {
                            handler.tick_count += 1;
                            handler.last_tick_ns = now;
                        }
                    }
                    Err(TrySendError::Full(_)) => {
                        self.total_dropped.fetch_add(1, Ordering::Relaxed);
                        self.metrics.dropped_intents.fetch_add(1, Ordering::Relaxed);
                        if let Some(handler) = self.handlers.get_mut(&symbol_id) {
                            handler.dropped_count += 1;
                            if handler.dropped_count % self.backpressure_log_interval == 0 {
                                tracing::warn!(
                                    symbol_id = %symbol_id,
                                    trace_id = trace_id,
                                    dropped = handler.dropped_count,
                                    "Back-pressure: tick channel full"
                                );
                            }
                        }
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        tracing::warn!(symbol_id = %symbol_id, trace_id = trace_id, "Handler disconnected");
                    }
                }
            } else {
                tracing::debug!(symbol_id = %symbol_id, trace_id = trace_id, "Received tick for unregistered symbol_id");
            }
        }
    }

    pub fn get_handler_stats(&self) -> HashMap<SymbolId, (u64, u64)> {
        self.handlers
            .iter()
            .map(|(sid, h)| (*sid, (h.tick_count, h.dropped_count)))
            .collect()
    }

    pub fn subscribed_symbols(&self) -> Vec<String> {
        self.handlers
            .keys()
            .filter_map(|sid| self.registry.get_symbol(*sid).map(|s| s.to_string()))
            .collect()
    }

    pub fn control_tx(&self) -> Sender<ReactorCommand> {
        self.control_tx.clone()
    }

    pub fn total_ticks(&self) -> u64 {
        self.total_ticks.load(Ordering::Relaxed)
    }

    pub fn total_dropped(&self) -> u64 {
        self.total_dropped.load(Ordering::Relaxed)
    }
}

pub fn spawn_reactor(
    tick_rx: Receiver<RawTick>,
    kill_switch: Arc<KillSwitch>,
    metrics: Arc<GlobalMetrics>,
    core_id: usize,
    max_batch_size: usize,
    control_batch_size: usize,
    sleep_on_empty_us: u64,
    backpressure_log_interval: u64,
) -> (Sender<ReactorCommand>, std::thread::JoinHandle<()>) {
    let (mut reactor, control_tx) = TickReactor::new(
        tick_rx, kill_switch, metrics,
        max_batch_size, control_batch_size, sleep_on_empty_us, backpressure_log_interval,
    );

    let handle = spawn_pinned(
        "tick-reactor",
        core_id,
        ThreadPriority::High,
        move || {
            reactor.run();
        },
    ).expect("spawn_pinned failed");

    (control_tx, handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_reactor(tick_rx: Receiver<RawTick>) -> (TickReactor, Sender<ReactorCommand>) {
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        TickReactor::new(tick_rx, kill_switch, metrics, 64, 16, 10, 1000)
    }

    #[test]
    fn test_reactor_subscribe_and_dispatch() {
        let (tick_tx, tick_rx) = bounded::<RawTick>(1000);

        let (mut reactor, control_tx) = make_reactor(tick_rx);

        let (handler_tx, handler_rx) = bounded::<RawTick>(100);
        reactor.subscribe("AAPL".to_string(), handler_tx);

        let symbol_id = reactor.registry.lookup("AAPL").unwrap();
        let tick = RawTick {
            symbol_id,
            timestamp_ns: 0,
            bid: 150.0,
            ask: 150.01,
            bid_size: 100,
            ask_size: 100,
            last_price: 150.0,
            last_size: 100,
            exchange: "V".to_string(),
            trace_id: 0,
        };

        tick_tx.send(tick.clone()).unwrap();

        reactor.process_tick_batch();

        let received = handler_rx.try_recv().unwrap();
        assert_eq!(received.symbol_id, symbol_id);
        assert!(received.trace_id > 0);
    }

    #[test]
    fn test_reactor_unsubscribe() {
        let (tick_tx, tick_rx) = bounded::<RawTick>(1000);

        let (mut reactor, _control_tx) = make_reactor(tick_rx);

        let (handler_tx, _handler_rx) = bounded::<RawTick>(100);
        reactor.subscribe("AAPL".to_string(), handler_tx);
        assert_eq!(reactor.subscribed_symbols().len(), 1);

        reactor.unsubscribe("AAPL");
        assert_eq!(reactor.subscribed_symbols().len(), 0);

        let symbol_id = SymbolId::from_raw(0);
        let tick = RawTick {
            symbol_id,
            timestamp_ns: 0,
            bid: 150.0,
            ask: 150.01,
            bid_size: 100,
            ask_size: 100,
            last_price: 150.0,
            last_size: 100,
            exchange: "V".to_string(),
            trace_id: 0,
        };

        tick_tx.send(tick).unwrap();
        reactor.process_tick_batch();
        assert_eq!(reactor.total_dropped(), 0);
    }

    #[test]
    fn test_reactor_back_pressure() {
        let (tick_tx, tick_rx) = bounded::<RawTick>(1000);

        let (mut reactor, _control_tx) = make_reactor(tick_rx);

        let (handler_tx, _handler_rx) = bounded::<RawTick>(1);
        reactor.subscribe("AAPL".to_string(), handler_tx);

        let symbol_id = reactor.registry.lookup("AAPL").unwrap();
        let tick = RawTick {
            symbol_id,
            timestamp_ns: 0,
            bid: 150.0,
            ask: 150.01,
            bid_size: 100,
            ask_size: 100,
            last_price: 150.0,
            last_size: 100,
            exchange: "V".to_string(),
            trace_id: 0,
        };

        tick_tx.send(tick.clone()).unwrap();
        tick_tx.send(tick.clone()).unwrap();

        reactor.process_tick_batch();

        assert!(reactor.total_dropped() > 0);
    }
}
