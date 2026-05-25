use std::sync::atomic::{AtomicU64, AtomicI64, Ordering};
use std::collections::HashMap;

/// Histogram buckets for latency measurements (in nanoseconds)
/// Buckets: <1us, <10us, <100us, <1ms, <10ms, >10ms
pub const LATENCY_BUCKETS: [u64; 6] = [1_000, 10_000, 100_000, 1_000_000, 10_000_000, u64::MAX];

pub struct LatencyHistogram {
    pub buckets: [AtomicU64; 6],
}

impl LatencyHistogram {
    pub fn new() -> Self {
        Self {
            buckets: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
        }
    }

    pub fn record(&self, latency_ns: u64) {
        for (i, &threshold) in LATENCY_BUCKETS.iter().enumerate() {
            if latency_ns < threshold {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        // If none matched, it goes in the last bucket (>10ms)
        self.buckets[5].fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> [u64; 6] {
        [
            self.buckets[0].load(Ordering::Relaxed),
            self.buckets[1].load(Ordering::Relaxed),
            self.buckets[2].load(Ordering::Relaxed),
            self.buckets[3].load(Ordering::Relaxed),
            self.buckets[4].load(Ordering::Relaxed),
            self.buckets[5].load(Ordering::Relaxed),
        ]
    }

    pub fn reset(&self) {
        for bucket in &self.buckets {
            bucket.store(0, Ordering::Relaxed);
        }
    }
}

pub struct GlobalMetrics {
    pub ticks_processed: AtomicU64,
    pub features_computed: AtomicU64,
    pub inferences_run: AtomicU64,
    pub intents_generated: AtomicU64,
    pub intents_approved: AtomicU64,
    pub intents_rejected: AtomicU64,
    pub dropped_intents: AtomicU64,
    pub stale_predictions: AtomicU64,
    pub orders_submitted: AtomicU64,
    pub orders_filled: AtomicU64,
    pub orders_cancelled: AtomicU64,
    pub orders_rejected: AtomicU64,
    pub orders_lifecycle_events: AtomicU64,
    pub circuit_breaker_trips: AtomicU64,
    pub kill_switch_activations: AtomicU64,
    pub config_reloads: AtomicU64,
    pub journal_writes: AtomicU64,
    pub heartbeat_misses: AtomicU64,
    pub errors: AtomicU64,
    // Latency histograms
    pub tick_to_intent_latency: LatencyHistogram,
    pub risk_check_latency: LatencyHistogram,
    pub journal_flush_latency: LatencyHistogram,
    pub broker_send_latency: LatencyHistogram,
    pub feed_latency: LatencyHistogram,
    pub broker_round_trip_latency: LatencyHistogram,
    // Channel depth gauges (approximate, since crossbeam doesn't expose len())
    pub feature_channel_depth: AtomicI64,
    pub risk_channel_depth: AtomicI64,
    pub decision_channel_depth: AtomicI64,
    pub lifecycle_channel_depth: AtomicI64,
    pub command_channel_depth: AtomicI64,
    pub journal_channel_depth: AtomicI64,
    // Per-symbol counters
    pub per_symbol_ticks: std::sync::Mutex<HashMap<String, AtomicU64>>,
    pub per_symbol_features: std::sync::Mutex<HashMap<String, AtomicU64>>,
    pub per_symbol_intents_approved: std::sync::Mutex<HashMap<String, AtomicU64>>,
    pub per_symbol_intents_rejected: std::sync::Mutex<HashMap<String, AtomicU64>>,
}

impl GlobalMetrics {
    pub fn new() -> Self {
        Self {
            ticks_processed: AtomicU64::new(0),
            features_computed: AtomicU64::new(0),
            inferences_run: AtomicU64::new(0),
            intents_generated: AtomicU64::new(0),
            intents_approved: AtomicU64::new(0),
            intents_rejected: AtomicU64::new(0),
            dropped_intents: AtomicU64::new(0),
            stale_predictions: AtomicU64::new(0),
            orders_submitted: AtomicU64::new(0),
            orders_filled: AtomicU64::new(0),
            orders_cancelled: AtomicU64::new(0),
            orders_rejected: AtomicU64::new(0),
            orders_lifecycle_events: AtomicU64::new(0),
            circuit_breaker_trips: AtomicU64::new(0),
            kill_switch_activations: AtomicU64::new(0),
            config_reloads: AtomicU64::new(0),
            journal_writes: AtomicU64::new(0),
            heartbeat_misses: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            tick_to_intent_latency: LatencyHistogram::new(),
            risk_check_latency: LatencyHistogram::new(),
            journal_flush_latency: LatencyHistogram::new(),
            broker_send_latency: LatencyHistogram::new(),
            feed_latency: LatencyHistogram::new(),
            broker_round_trip_latency: LatencyHistogram::new(),
            feature_channel_depth: AtomicI64::new(0),
            risk_channel_depth: AtomicI64::new(0),
            decision_channel_depth: AtomicI64::new(0),
            lifecycle_channel_depth: AtomicI64::new(0),
            command_channel_depth: AtomicI64::new(0),
            journal_channel_depth: AtomicI64::new(0),
            per_symbol_ticks: std::sync::Mutex::new(HashMap::new()),
            per_symbol_features: std::sync::Mutex::new(HashMap::new()),
            per_symbol_intents_approved: std::sync::Mutex::new(HashMap::new()),
            per_symbol_intents_rejected: std::sync::Mutex::new(HashMap::new()),
        }
    }

    pub fn reset(&self) {
        self.ticks_processed.store(0, Ordering::Relaxed);
        self.features_computed.store(0, Ordering::Relaxed);
        self.inferences_run.store(0, Ordering::Relaxed);
        self.intents_generated.store(0, Ordering::Relaxed);
        self.intents_approved.store(0, Ordering::Relaxed);
        self.intents_rejected.store(0, Ordering::Relaxed);
        self.dropped_intents.store(0, Ordering::Relaxed);
        self.stale_predictions.store(0, Ordering::Relaxed);
        self.orders_submitted.store(0, Ordering::Relaxed);
        self.orders_filled.store(0, Ordering::Relaxed);
        self.orders_cancelled.store(0, Ordering::Relaxed);
        self.orders_rejected.store(0, Ordering::Relaxed);
        self.orders_lifecycle_events.store(0, Ordering::Relaxed);
        self.circuit_breaker_trips.store(0, Ordering::Relaxed);
        self.kill_switch_activations.store(0, Ordering::Relaxed);
        self.config_reloads.store(0, Ordering::Relaxed);
        self.journal_writes.store(0, Ordering::Relaxed);
        self.heartbeat_misses.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        self.tick_to_intent_latency.reset();
        self.risk_check_latency.reset();
        self.journal_flush_latency.reset();
        self.broker_send_latency.reset();
        self.feed_latency.reset();
        self.broker_round_trip_latency.reset();
        self.feature_channel_depth.store(0, Ordering::Relaxed);
        self.risk_channel_depth.store(0, Ordering::Relaxed);
        self.decision_channel_depth.store(0, Ordering::Relaxed);
        self.lifecycle_channel_depth.store(0, Ordering::Relaxed);
        self.command_channel_depth.store(0, Ordering::Relaxed);
        self.journal_channel_depth.store(0, Ordering::Relaxed);
        self.per_symbol_ticks.lock().unwrap().clear();
        self.per_symbol_features.lock().unwrap().clear();
        self.per_symbol_intents_approved.lock().unwrap().clear();
        self.per_symbol_intents_rejected.lock().unwrap().clear();
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let per_symbol_ticks = {
            let map = self.per_symbol_ticks.lock().unwrap();
            map.iter()
                .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
                .collect()
        };
        let per_symbol_features = {
            let map = self.per_symbol_features.lock().unwrap();
            map.iter()
                .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
                .collect()
        };
        let per_symbol_intents_approved = {
            let map = self.per_symbol_intents_approved.lock().unwrap();
            map.iter()
                .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
                .collect()
        };
        let per_symbol_intents_rejected = {
            let map = self.per_symbol_intents_rejected.lock().unwrap();
            map.iter()
                .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
                .collect()
        };
        MetricsSnapshot {
            ticks_processed: self.ticks_processed.load(Ordering::Relaxed),
            features_computed: self.features_computed.load(Ordering::Relaxed),
            inferences_run: self.inferences_run.load(Ordering::Relaxed),
            intents_generated: self.intents_generated.load(Ordering::Relaxed),
            intents_approved: self.intents_approved.load(Ordering::Relaxed),
            intents_rejected: self.intents_rejected.load(Ordering::Relaxed),
            dropped_intents: self.dropped_intents.load(Ordering::Relaxed),
            stale_predictions: self.stale_predictions.load(Ordering::Relaxed),
            orders_submitted: self.orders_submitted.load(Ordering::Relaxed),
            orders_filled: self.orders_filled.load(Ordering::Relaxed),
            orders_cancelled: self.orders_cancelled.load(Ordering::Relaxed),
            orders_rejected: self.orders_rejected.load(Ordering::Relaxed),
            orders_lifecycle_events: self.orders_lifecycle_events.load(Ordering::Relaxed),
            circuit_breaker_trips: self.circuit_breaker_trips.load(Ordering::Relaxed),
            kill_switch_activations: self.kill_switch_activations.load(Ordering::Relaxed),
            config_reloads: self.config_reloads.load(Ordering::Relaxed),
            journal_writes: self.journal_writes.load(Ordering::Relaxed),
            heartbeat_misses: self.heartbeat_misses.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            tick_to_intent_latency: self.tick_to_intent_latency.snapshot(),
            risk_check_latency: self.risk_check_latency.snapshot(),
            journal_flush_latency: self.journal_flush_latency.snapshot(),
            broker_send_latency: self.broker_send_latency.snapshot(),
            feed_latency: self.feed_latency.snapshot(),
            broker_round_trip_latency: self.broker_round_trip_latency.snapshot(),
            feature_channel_depth: self.feature_channel_depth.load(Ordering::Relaxed),
            risk_channel_depth: self.risk_channel_depth.load(Ordering::Relaxed),
            decision_channel_depth: self.decision_channel_depth.load(Ordering::Relaxed),
            lifecycle_channel_depth: self.lifecycle_channel_depth.load(Ordering::Relaxed),
            command_channel_depth: self.command_channel_depth.load(Ordering::Relaxed),
            journal_channel_depth: self.journal_channel_depth.load(Ordering::Relaxed),
            per_symbol_ticks,
            per_symbol_features,
            per_symbol_intents_approved,
            per_symbol_intents_rejected,
        }
    }

    pub fn increment_per_symbol_tick(&self, symbol: &str) {
        let mut map = self.per_symbol_ticks.lock().unwrap();
        map.entry(symbol.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_per_symbol_feature(&self, symbol: &str) {
        let mut map = self.per_symbol_features.lock().unwrap();
        map.entry(symbol.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_per_symbol_intent_approved(&self, symbol: &str) {
        let mut map = self.per_symbol_intents_approved.lock().unwrap();
        map.entry(symbol.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_per_symbol_intent_rejected(&self, symbol: &str) {
        let mut map = self.per_symbol_intents_rejected.lock().unwrap();
        map.entry(symbol.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetricsSnapshot {
    pub ticks_processed: u64,
    pub features_computed: u64,
    pub inferences_run: u64,
    pub intents_generated: u64,
    pub intents_approved: u64,
    pub intents_rejected: u64,
    pub dropped_intents: u64,
    pub stale_predictions: u64,
    pub orders_submitted: u64,
    pub orders_filled: u64,
    pub orders_cancelled: u64,
    pub orders_rejected: u64,
    pub orders_lifecycle_events: u64,
    pub circuit_breaker_trips: u64,
    pub kill_switch_activations: u64,
    pub config_reloads: u64,
    pub journal_writes: u64,
    pub heartbeat_misses: u64,
    pub errors: u64,
    // Latency histograms (buckets: <1us, <10us, <100us, <1ms, <10ms, >10ms)
    pub tick_to_intent_latency: [u64; 6],
    pub risk_check_latency: [u64; 6],
    pub journal_flush_latency: [u64; 6],
    pub broker_send_latency: [u64; 6],
    pub feed_latency: [u64; 6],
    pub broker_round_trip_latency: [u64; 6],
    // Channel depth gauges
    pub feature_channel_depth: i64,
    pub risk_channel_depth: i64,
    pub decision_channel_depth: i64,
    pub lifecycle_channel_depth: i64,
    pub command_channel_depth: i64,
    pub journal_channel_depth: i64,
    // Per-symbol counters
    pub per_symbol_ticks: std::collections::HashMap<String, u64>,
    pub per_symbol_features: std::collections::HashMap<String, u64>,
    pub per_symbol_intents_approved: std::collections::HashMap<String, u64>,
    pub per_symbol_intents_rejected: std::collections::HashMap<String, u64>,
}

impl Default for GlobalMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_initial_state() {
        let m = GlobalMetrics::new();
        let snap = m.snapshot();
        assert_eq!(snap.ticks_processed, 0);
        assert_eq!(snap.errors, 0);
    }

    #[test]
    fn test_metrics_increment() {
        let m = GlobalMetrics::new();
        m.ticks_processed.fetch_add(1, Ordering::Relaxed);
        m.ticks_processed.fetch_add(5, Ordering::Relaxed);
        assert_eq!(m.ticks_processed.load(Ordering::Relaxed), 6);
    }

    #[test]
    fn test_metrics_reset() {
        let m = GlobalMetrics::new();
        m.ticks_processed.fetch_add(100, Ordering::Relaxed);
        m.errors.fetch_add(5, Ordering::Relaxed);
        m.reset();
        let snap = m.snapshot();
        assert_eq!(snap.ticks_processed, 0);
        assert_eq!(snap.errors, 0);
    }

    #[test]
    fn test_metrics_snapshot() {
        let m = GlobalMetrics::new();
        m.intents_approved.fetch_add(10, Ordering::Relaxed);
        m.intents_rejected.fetch_add(3, Ordering::Relaxed);
        let snap = m.snapshot();
        assert_eq!(snap.intents_approved, 10);
        assert_eq!(snap.intents_rejected, 3);
    }
}
