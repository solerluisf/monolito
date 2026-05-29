use crossbeam_channel::{bounded, Receiver, Sender};
use std::thread;

use crate::normalizer::{RawTick, TickType};

pub struct MarketDataReceiver {
    tx: Sender<RawTick>,
    pub rx: Receiver<RawTick>,
    handle: Option<thread::JoinHandle<()>>,
    running: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl MarketDataReceiver {
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = bounded(capacity);
        let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));

        Self {
            tx,
            rx,
            handle: None,
            running,
        }
    }

    pub fn sender(&self) -> Sender<RawTick> {
        self.tx.clone()
    }

    pub fn recv_batch(&self, buf: &mut Vec<RawTick>, max: usize) -> usize {
        buf.clear();
        match self.rx.try_recv() {
            Ok(item) => buf.push(item),
            Err(_) => return 0,
        }
        for _ in 1..max {
            match self.rx.try_recv() {
                Ok(item) => buf.push(item),
                Err(_) => break,
            }
        }
        buf.len()
    }

    pub fn is_running(&self) -> bool {
        self.running.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn stop(&mut self) {
        self.running.store(false, std::sync::atomic::Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use unified_trading_core::symbol_registry::SymbolId;

    #[test]
    fn test_recv_batch_empty() {
        let receiver = MarketDataReceiver::new(100);
        let mut buf = Vec::with_capacity(32);
        let count = receiver.recv_batch(&mut buf, 32);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_recv_batch_with_data() {
        let receiver = MarketDataReceiver::new(100);
        let tx = receiver.sender();

        for i in 0..10 {
            tx.send(RawTick {
                symbol_id: SymbolId::from_raw(0),
                symbol: "TEST".to_string(),
                tick_type: TickType::Quote,
                timestamp_ns: i,
                bid: 150.0,
                ask: 150.05,
                bid_size: 100,
                ask_size: 200,
                last_price: 150.02,
                last_size: 50,
                exchange: "IEX".to_string(),
                trace_id: i as u64,
            })
            .unwrap();
        }

        let mut buf = Vec::with_capacity(32);
        let count = receiver.recv_batch(&mut buf, 32);
        assert_eq!(count, 10);
    }

    #[test]
    fn test_recv_batch_respects_max() {
        let receiver = MarketDataReceiver::new(100);
        let tx = receiver.sender();

        for i in 0..20 {
            tx.send(RawTick {
                symbol_id: SymbolId::from_raw(0),
                symbol: "TEST".to_string(),
                tick_type: TickType::Quote,
                timestamp_ns: i,
                bid: 150.0,
                ask: 150.05,
                bid_size: 100,
                ask_size: 200,
                last_price: 150.02,
                last_size: 50,
                exchange: "IEX".to_string(),
                trace_id: i as u64,
            })
            .unwrap();
        }

        let mut buf = Vec::with_capacity(5);
        let count = receiver.recv_batch(&mut buf, 5);
        assert_eq!(count, 5);
    }
}
