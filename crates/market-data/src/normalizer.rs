use serde::{Deserialize, Serialize};
use unified_trading_core::symbol_registry::SymbolId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TickType {
    Quote,
    Trade,
    Bar,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawTick {
    pub symbol_id: SymbolId,
    pub symbol: String,
    pub tick_type: TickType,
    pub timestamp_ns: u64,
    pub bid: f64,
    pub ask: f64,
    pub bid_size: u64,
    pub ask_size: u64,
    pub last_price: f64,
    pub last_size: u64,
    pub exchange: String,
    /// Unique trace ID for causal tracing across the pipeline.
    /// Generated at tick ingestion and propagated through all subsequent stages.
    pub trace_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedTick {
    pub symbol_id: SymbolId,
    pub timestamp_ns: u64,
    pub mid_price: f64,
    pub spread: f64,
    pub spread_bps: f64,
    pub bid: f64,
    pub ask: f64,
    pub bid_size: u64,
    pub ask_size: u64,
    pub volume: u64,
    /// Trace ID propagated from RawTick for causal tracing.
    pub trace_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedAlert {
    pub symbol_id: SymbolId,
    pub timestamp_ns: u64,
    pub alert_type: FeedAlertType,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FeedAlertType {
    StaleData,
    SequenceGap,
    LatencySpike,
    PriceAnomaly,
}

pub struct Normalizer {
    symbol_id: SymbolId,
    last_sequence: u64,
    last_timestamp_ns: u64,
    last_spread: Option<f64>,
}

impl Normalizer {
    pub fn new(symbol_id: SymbolId) -> Self {
        Self {
            symbol_id,
            last_sequence: 0,
            last_timestamp_ns: 0,
            last_spread: None,
        }
    }

    /// Returns `None` if the tick contains invalid prices (NaN, Inf, non-positive, or crossed book).
    pub fn process(&mut self, raw: RawTick) -> Option<(NormalizedTick, bool)> {
        if !Self::validate_tick(&raw) {
            tracing::warn!(
                symbol = %raw.symbol,
                symbol_id = %raw.symbol_id,
                tick_type = ?raw.tick_type,
                bid = %raw.bid,
                ask = %raw.ask,
                last_price = %raw.last_price,
                "Rejected malformed tick: NaN, Inf, non-positive price, or crossed book"
            );
            return None;
        }

        let (mid_price, spread, spread_bps) = match raw.tick_type {
            TickType::Quote => {
                let mid = (raw.bid + raw.ask) / 2.0;
                let sp = raw.ask - raw.bid;
                let sp_bps = if mid > 0.0 { (sp / mid) * 10_000.0 } else { 0.0 };
                (mid, sp, sp_bps)
            }
            TickType::Trade => {
                let mid = raw.last_price;
                let sp = self.last_spread.unwrap_or(0.0);
                let sp_bps = if mid > 0.0 { (sp / mid) * 10_000.0 } else { 0.0 };
                (mid, sp, sp_bps)
            }
            TickType::Bar => {
                let mid = raw.last_price;
                let sp = 0.0;
                let sp_bps = 0.0;
                (mid, sp, sp_bps)
            }
        };

        if raw.tick_type == TickType::Quote && spread > 0.0 {
            self.last_spread = Some(spread);
        }

        self.last_sequence += 1;
        let gap = self.last_timestamp_ns > 0 && raw.timestamp_ns > self.last_timestamp_ns + 1_000_000_000;
        self.last_timestamp_ns = raw.timestamp_ns;

        Some((NormalizedTick {
            symbol_id: raw.symbol_id,
            timestamp_ns: raw.timestamp_ns,
            mid_price,
            spread,
            spread_bps,
            bid: raw.bid,
            ask: raw.ask,
            bid_size: raw.bid_size,
            ask_size: raw.ask_size,
            volume: raw.last_size,
            trace_id: raw.trace_id,
        }, gap))
    }

    fn validate_tick(raw: &RawTick) -> bool {
        for &price in &[raw.bid, raw.ask, raw.last_price] {
            if price.is_nan() || price.is_infinite() || price <= 0.0 {
                return false;
            }
        }
        match raw.tick_type {
            TickType::Quote => raw.bid <= raw.ask,
            TickType::Trade | TickType::Bar => true,
        }
    }

    pub fn check_sequence(&self, expected: u64) -> Option<u64> {
        if expected != self.last_sequence + 1 {
            Some(expected.saturating_sub(self.last_sequence))
        } else {
            None
        }
    }

    pub fn check_staleness(&self, now_ns: u64, threshold_ns: u64) -> bool {
        now_ns.saturating_sub(self.last_timestamp_ns) > threshold_ns
    }
}

pub struct FeedMonitor {
    symbol_id: SymbolId,
    tick_count: u64,
    last_tick_ns: u64,
    max_latency_ns: u64,
    alerts: Vec<FeedAlert>,
}

impl FeedMonitor {
    pub fn new(symbol_id: SymbolId) -> Self {
        Self {
            symbol_id,
            tick_count: 0,
            last_tick_ns: 0,
            max_latency_ns: 0,
            alerts: Vec::new(),
        }
    }

    pub fn on_tick(&mut self, timestamp_ns: u64, received_ns: u64) {
        self.tick_count += 1;
        let latency = received_ns.saturating_sub(timestamp_ns);
        if latency > self.max_latency_ns {
            self.max_latency_ns = latency;
        }
        self.last_tick_ns = received_ns;
    }

    pub fn check_latency(&self, now_ns: u64, threshold_ns: u64) -> Option<FeedAlert> {
        let age = now_ns.saturating_sub(self.last_tick_ns);
        if age > threshold_ns {
            Some(FeedAlert {
                symbol_id: self.symbol_id,
                timestamp_ns: now_ns,
                alert_type: FeedAlertType::LatencySpike,
                message: format!("Feed latency {}ns exceeds threshold {}ns", age, threshold_ns),
            })
        } else {
            None
        }
    }

    pub fn tick_count(&self) -> u64 {
        self.tick_count
    }

    pub fn max_latency_ns(&self) -> u64 {
        self.max_latency_ns
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use unified_trading_core::symbol_registry::SymbolId;

    #[test]
    fn test_normalizer_basic() {
        let symbol_id = SymbolId::from_raw(0);
        let mut norm = Normalizer::new(symbol_id);
        let raw = RawTick {
            symbol_id,
            symbol: "AAPL".to_string(),
            tick_type: TickType::Quote,
            timestamp_ns: 1000,
            bid: 150.0,
            ask: 150.05,
            bid_size: 100,
            ask_size: 200,
            last_price: 150.02,
            last_size: 50,
            exchange: "IEX".to_string(),
            trace_id: 1,
        };
        let (normalized, _gap) = norm.process(raw).unwrap();
        assert_eq!(normalized.symbol_id, symbol_id);
        assert!((normalized.mid_price - 150.025).abs() < 0.001);
        assert!((normalized.spread - 0.05).abs() < 0.001);
        assert_eq!(normalized.trace_id, 1);
    }

    #[test]
    fn test_normalizer_spread_bps() {
        let symbol_id = SymbolId::from_raw(1);
        let mut norm = Normalizer::new(symbol_id);
        let raw = RawTick {
            symbol_id,
            symbol: "AAPL".to_string(),
            tick_type: TickType::Quote,
            timestamp_ns: 2000,
            bid: 400.0,
            ask: 400.04,
            bid_size: 50,
            ask_size: 50,
            last_price: 400.02,
            last_size: 10,
            exchange: "IEX".to_string(),
            trace_id: 2,
        };
        let (normalized, _gap) = norm.process(raw).unwrap();
        assert!((normalized.spread_bps - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_normalizer_sequence_check() {
        let symbol_id = SymbolId::from_raw(0);
        let mut norm = Normalizer::new(symbol_id);
        norm.process(RawTick {
            symbol_id,
            symbol: "AAPL".to_string(),
            tick_type: TickType::Quote,
            timestamp_ns: 1000,
            bid: 150.0,
            ask: 150.05,
            bid_size: 100,
            ask_size: 200,
            last_price: 150.02,
            last_size: 50,
            exchange: "IEX".to_string(),
            trace_id: 3,
        }).unwrap();
        assert!(norm.check_sequence(2).is_none());
        assert!(norm.check_sequence(5).is_some());
    }

    #[test]
    fn test_normalizer_staleness() {
        let symbol_id = SymbolId::from_raw(0);
        let mut norm = Normalizer::new(symbol_id);
        norm.process(RawTick {
            symbol_id,
            symbol: "AAPL".to_string(),
            tick_type: TickType::Trade,
            timestamp_ns: 1_000_000_000,
            bid: 150.0,
            ask: 150.05,
            bid_size: 100,
            ask_size: 200,
            last_price: 150.02,
            last_size: 50,
            exchange: "IEX".to_string(),
            trace_id: 4,
        }).unwrap();
        assert!(!norm.check_staleness(1_100_000_000, 200_000_000));
        assert!(norm.check_staleness(1_300_000_000, 200_000_000));
    }

    #[test]
    fn test_feed_monitor() {
        let symbol_id = SymbolId::from_raw(0);
        let mut monitor = FeedMonitor::new(symbol_id);
        monitor.on_tick(1000, 2000);
        monitor.on_tick(2000, 2500);
        assert_eq!(monitor.tick_count(), 2);
        assert_eq!(monitor.max_latency_ns(), 1000);
    }

    #[test]
    fn test_feed_monitor_latency_alert() {
        let symbol_id = SymbolId::from_raw(0);
        let mut monitor = FeedMonitor::new(symbol_id);
        monitor.on_tick(1000, 1500);
        let alert = monitor.check_latency(10_000_000, 5_000_000);
        assert!(alert.is_some());
        let alert = alert.unwrap();
        assert!(matches!(alert.alert_type, FeedAlertType::LatencySpike));
    }

    #[test]
    fn test_normalizer_rejects_nan_bid() {
        let symbol_id = SymbolId::from_raw(0);
        let mut norm = Normalizer::new(symbol_id);
        let raw = RawTick {
            symbol_id,
            symbol: "AAPL".to_string(),
            tick_type: TickType::Quote,
            timestamp_ns: 1000,
            bid: f64::NAN,
            ask: 150.05,
            bid_size: 100,
            ask_size: 200,
            last_price: 150.02,
            last_size: 50,
            exchange: "IEX".to_string(),
            trace_id: 5,
        };
        assert!(norm.process(raw).is_none());
    }

    #[test]
    fn test_normalizer_rejects_inf_ask() {
        let symbol_id = SymbolId::from_raw(0);
        let mut norm = Normalizer::new(symbol_id);
        let raw = RawTick {
            symbol_id,
            symbol: "AAPL".to_string(),
            tick_type: TickType::Quote,
            timestamp_ns: 1000,
            bid: 150.0,
            ask: f64::INFINITY,
            bid_size: 100,
            ask_size: 200,
            last_price: 150.02,
            last_size: 50,
            exchange: "IEX".to_string(),
            trace_id: 6,
        };
        assert!(norm.process(raw).is_none());
    }

    #[test]
    fn test_normalizer_rejects_zero_price() {
        let symbol_id = SymbolId::from_raw(0);
        let mut norm = Normalizer::new(symbol_id);
        let raw = RawTick {
            symbol_id,
            symbol: "AAPL".to_string(),
            tick_type: TickType::Quote,
            timestamp_ns: 1000,
            bid: 150.0,
            ask: 150.05,
            bid_size: 100,
            ask_size: 200,
            last_price: 0.0,
            last_size: 50,
            exchange: "IEX".to_string(),
            trace_id: 7,
        };
        assert!(norm.process(raw).is_none());
    }

    #[test]
    fn test_normalizer_rejects_negative_bid() {
        let symbol_id = SymbolId::from_raw(0);
        let mut norm = Normalizer::new(symbol_id);
        let raw = RawTick {
            symbol_id,
            symbol: "AAPL".to_string(),
            tick_type: TickType::Quote,
            timestamp_ns: 1000,
            bid: -1.0,
            ask: 150.05,
            bid_size: 100,
            ask_size: 200,
            last_price: 150.02,
            last_size: 50,
            exchange: "IEX".to_string(),
            trace_id: 8,
        };
        assert!(norm.process(raw).is_none());
    }

    #[test]
    fn test_normalizer_rejects_crossed_book() {
        let symbol_id = SymbolId::from_raw(0);
        let mut norm = Normalizer::new(symbol_id);
        let raw = RawTick {
            symbol_id,
            symbol: "AAPL".to_string(),
            tick_type: TickType::Quote,
            timestamp_ns: 1000,
            bid: 160.0,
            ask: 150.05,
            bid_size: 100,
            ask_size: 200,
            last_price: 150.02,
            last_size: 50,
            exchange: "IEX".to_string(),
            trace_id: 9,
        };
        assert!(norm.process(raw).is_none());
    }

    #[test]
    fn test_normalizer_accepts_valid_tick() {
        let symbol_id = SymbolId::from_raw(0);
        let mut norm = Normalizer::new(symbol_id);
        let raw = RawTick {
            symbol_id,
            symbol: "AAPL".to_string(),
            tick_type: TickType::Quote,
            timestamp_ns: 1000,
            bid: 150.0,
            ask: 150.05,
            bid_size: 100,
            ask_size: 200,
            last_price: 150.02,
            last_size: 50,
            exchange: "IEX".to_string(),
            trace_id: 10,
        };
        let result = norm.process(raw);
        assert!(result.is_some());
        let (normalized, _) = result.unwrap();
        assert!((normalized.mid_price - 150.025).abs() < 0.001);
    }
}
