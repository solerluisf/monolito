use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};

use unified_trading_core::kill_switch::KillSwitch;
use unified_trading_core::metrics::GlobalMetrics;
use unified_trading_core::symbol_registry::{SymbolRegistry, SymbolIdArray};

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
    handlers: HashMap<String, SymbolHandler>,
    registry: SymbolRegistry,
    handler_array: SymbolIdArray<Sender<RawTick>>,
    kill_switch: Arc<KillSwitch>,
    metrics: Arc<GlobalMetrics>,
    running: Arc<AtomicBool>,
    total_ticks: Arc<AtomicU64>,
    total_dropped: Arc<AtomicU64>,
    max_batch_size: usize,
}

impl TickReactor {
    pub fn new(
        tick_rx: Receiver<RawTick>,
        kill_switch: Arc<KillSwitch>,
        metrics: Arc<GlobalMetrics>,
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
            max_batch_size: 64,
        };

        (reactor, control_tx)
    }

    pub fn subscribe(&mut self, symbol: String, tx: Sender<RawTick>) {
        if self.registry.lookup(&symbol).is_some() {
            tracing::warn!("Symbol {} already subscribed, replacing handler", symbol);
            self.handlers.remove(&symbol);
        }

        if let Some(id) = self.registry.register(&symbol) {
            self.handler_array.set(id, tx.clone());
            self.handlers.insert(symbol.clone(), SymbolHandler {
                tx,
                tick_count: 0,
                last_tick_ns: 0,
                dropped_count: 0,
            });
            tracing::info!("Subscribed to symbol {} (ID: {:?})", symbol, id);
        } else {
            tracing::error!("Failed to register symbol {} - registry full", symbol);
        }
    }

    pub fn unsubscribe(&mut self, symbol: &str) {
        if self.registry.lookup(symbol).is_some() {
            self.handlers.remove(symbol);
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
        for _ in 0..16 {
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
            std::thread::sleep(std::time::Duration::from_micros(10));
            return;
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        for tick in batch.drain(..) {
            self.total_ticks.fetch_add(1, Ordering::Relaxed);
            let symbol = tick.symbol.clone();

            if let Some(id) = self.registry.lookup(&symbol) {
                if let Some(tx) = self.handler_array.get(id) {
                    match tx.try_send(tick) {
                        Ok(()) => {
                            if let Some(handler) = self.handlers.get_mut(&symbol) {
                                handler.tick_count += 1;
                                handler.last_tick_ns = now;
                            }
                        }
                        Err(TrySendError::Full(_)) => {
                            self.total_dropped.fetch_add(1, Ordering::Relaxed);
                            self.metrics.dropped_intents.fetch_add(1, Ordering::Relaxed);
                            if let Some(handler) = self.handlers.get_mut(&symbol) {
                                handler.dropped_count += 1;
                                if handler.dropped_count % 1000 == 0 {
                                    tracing::warn!(
                                        symbol = %symbol,
                                        dropped = handler.dropped_count,
                                        "Back-pressure: tick channel full"
                                    );
                                }
                            }
                        }
                        Err(TrySendError::Disconnected(_)) => {
                            tracing::warn!("Handler for {} disconnected, unsubscribing", symbol);
                            self.unsubscribe(&symbol);
                        }
                    }
                }
            } else {
                tracing::debug!("Received tick for unregistered symbol: {}", symbol);
            }
        }
    }

    pub fn get_handler_stats(&self) -> HashMap<String, (u64, u64)> {
        self.handlers.iter()
            .map(|(sym, h)| (sym.clone(), (h.tick_count, h.dropped_count)))
            .collect()
    }

    pub fn subscribed_symbols(&self) -> Vec<String> {
        self.handlers.keys().cloned().collect()
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
) -> (Sender<ReactorCommand>, std::thread::JoinHandle<()>) {
    let (mut reactor, control_tx) = TickReactor::new(tick_rx, kill_switch, metrics);

    let handle = std::thread::Builder::new()
        .name("tick-reactor".to_string())
        .spawn(move || {
            reactor.run();
        })
        .expect("Failed to spawn tick reactor");

    (control_tx, handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reactor_subscribe_and_dispatch() {
        let (tick_tx, tick_rx) = bounded::<RawTick>(1000);
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());

        let (mut reactor, control_tx) = TickReactor::new(tick_rx, kill_switch, metrics);

        let (handler_tx, handler_rx) = bounded::<RawTick>(100);
        reactor.subscribe("AAPL".to_string(), handler_tx);

        let tick = RawTick {
            symbol: "AAPL".to_string(),
            timestamp_ns: 0,
            bid: 150.0,
            ask: 150.01,
            bid_size: 100,
            ask_size: 100,
            last_price: 150.0,
            last_size: 100,
            exchange: "V".to_string(),
        };

        tick_tx.send(tick.clone()).unwrap();

        reactor.process_tick_batch();

        let received = handler_rx.try_recv().unwrap();
        assert_eq!(received.symbol, "AAPL");
    }

    #[test]
    fn test_reactor_unsubscribe() {
        let (tick_tx, tick_rx) = bounded::<RawTick>(1000);
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());

        let (mut reactor, _control_tx) = TickReactor::new(tick_rx, kill_switch, metrics);

        let (handler_tx, _handler_rx) = bounded::<RawTick>(100);
        reactor.subscribe("AAPL".to_string(), handler_tx);
        assert_eq!(reactor.subscribed_symbols().len(), 1);

        reactor.unsubscribe("AAPL");
        assert_eq!(reactor.subscribed_symbols().len(), 0);

        let tick = RawTick {
            symbol: "AAPL".to_string(),
            timestamp_ns: 0,
            bid: 150.0,
            ask: 150.01,
            bid_size: 100,
            ask_size: 100,
            last_price: 150.0,
            last_size: 100,
            exchange: "V".to_string(),
        };

        tick_tx.send(tick).unwrap();
        reactor.process_tick_batch();
        assert_eq!(reactor.total_dropped(), 0);
    }

    #[test]
    fn test_reactor_back_pressure() {
        let (tick_tx, tick_rx) = bounded::<RawTick>(1000);
        let kill_switch = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());

        let (mut reactor, _control_tx) = TickReactor::new(tick_rx, kill_switch, metrics);

        let (handler_tx, _handler_rx) = bounded::<RawTick>(1);
        reactor.subscribe("AAPL".to_string(), handler_tx);

        let tick = RawTick {
            symbol: "AAPL".to_string(),
            timestamp_ns: 0,
            bid: 150.0,
            ask: 150.01,
            bid_size: 100,
            ask_size: 100,
            last_price: 150.0,
            last_size: 100,
            exchange: "V".to_string(),
        };

        tick_tx.send(tick.clone()).unwrap();
        tick_tx.send(tick.clone()).unwrap();

        reactor.process_tick_batch();

        assert!(reactor.total_dropped() > 0);
    }
}
