use std::sync::atomic::{AtomicU64, AtomicI64, Ordering};
use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use parking_lot::Mutex;
use crossbeam_channel::{bounded, Sender, Receiver};
use crate::threading::{spawn_pinned, ThreadPriority};

/// Cache-line padded atomic counter to reduce false sharing between hot fields.
#[repr(align(64))]
pub struct CachePaddedAtomicU64 {
    value: AtomicU64,
}

impl CachePaddedAtomicU64 {
    pub fn new(value: u64) -> Self {
        Self {
            value: AtomicU64::new(value),
        }
    }

    #[inline]
    pub fn fetch_add(&self, val: u64, ordering: Ordering) -> u64 {
        self.value.fetch_add(val, ordering)
    }

    #[inline]
    pub fn load(&self, ordering: Ordering) -> u64 {
        self.value.load(ordering)
    }

    #[inline]
    pub fn store(&self, val: u64, ordering: Ordering) {
        self.value.store(val, ordering);
    }
}

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

/// Batched metrics accumulator sent from thread-local to the aggregator thread.
/// This reduces atomic contention on the hot path by batching increments.
#[derive(Debug, Default, Clone)]
pub struct MetricsBatch {
    pub ticks_processed: u64,
    pub features_computed: u64,
    pub intents_generated: u64,
    pub intents_approved: u64,
    pub intents_rejected: u64,
    pub dropped_intents: u64,
    pub stale_predictions: u64,
    pub model_fallback_activations: u64,
    pub ticks_skipped: u64,
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
    pub feed_gaps: u64,
    // Latency histogram buckets (counts per bucket)
    pub tick_to_intent_latency: [u64; 6],
    pub risk_check_latency: [u64; 6],
    pub journal_flush_latency: [u64; 6],
    pub broker_send_latency: [u64; 6],
    pub feed_latency: [u64; 6],
    pub broker_round_trip_latency: [u64; 6],
    pub decision_latency: [u64; 6],
    // Model divergence histogram buckets (forecast delta, confidence delta)
    pub model_divergence: [u64; 6],
    // Channel depth deltas (positive = sent, negative = received)
    pub feature_channel_depth_delta: i64,
    pub risk_channel_depth_delta: i64,
    pub decision_channel_depth_delta: i64,
    pub lifecycle_channel_depth_delta: i64,
    pub command_channel_depth_delta: i64,
    pub journal_channel_depth_delta: i64,
    // Per-symbol counters
    pub per_symbol_ticks: HashMap<String, u64>,
    pub per_symbol_features: HashMap<String, u64>,
    pub per_symbol_intents_approved: HashMap<String, u64>,
    pub per_symbol_intents_rejected: HashMap<String, u64>,
    pub per_symbol_ticks_skipped: HashMap<String, u64>,
}

impl MetricsBatch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add another batch to this one (for aggregating multiple batches)
    pub fn merge(&mut self, other: &MetricsBatch) {
        self.ticks_processed += other.ticks_processed;
        self.features_computed += other.features_computed;
        self.intents_generated += other.intents_generated;
        self.intents_approved += other.intents_approved;
        self.intents_rejected += other.intents_rejected;
        self.dropped_intents += other.dropped_intents;
        self.stale_predictions += other.stale_predictions;
        self.model_fallback_activations += other.model_fallback_activations;
        self.ticks_skipped += other.ticks_skipped;
        self.orders_submitted += other.orders_submitted;
        self.orders_filled += other.orders_filled;
        self.orders_cancelled += other.orders_cancelled;
        self.orders_rejected += other.orders_rejected;
        self.orders_lifecycle_events += other.orders_lifecycle_events;
        self.circuit_breaker_trips += other.circuit_breaker_trips;
        self.kill_switch_activations += other.kill_switch_activations;
        self.config_reloads += other.config_reloads;
        self.journal_writes += other.journal_writes;
        self.heartbeat_misses += other.heartbeat_misses;
        self.errors += other.errors;
        self.feed_gaps += other.feed_gaps;

        for (i, &v) in other.tick_to_intent_latency.iter().enumerate() {
            self.tick_to_intent_latency[i] += v;
        }
        for (i, &v) in other.risk_check_latency.iter().enumerate() {
            self.risk_check_latency[i] += v;
        }
        for (i, &v) in other.journal_flush_latency.iter().enumerate() {
            self.journal_flush_latency[i] += v;
        }
        for (i, &v) in other.broker_send_latency.iter().enumerate() {
            self.broker_send_latency[i] += v;
        }
        for (i, &v) in other.feed_latency.iter().enumerate() {
            self.feed_latency[i] += v;
        }
        for (i, &v) in other.broker_round_trip_latency.iter().enumerate() {
            self.broker_round_trip_latency[i] += v;
        }
        for (i, &v) in other.decision_latency.iter().enumerate() {
            self.decision_latency[i] += v;
        }
        for (i, &v) in other.model_divergence.iter().enumerate() {
            self.model_divergence[i] += v;
        }

        self.feature_channel_depth_delta += other.feature_channel_depth_delta;
        self.risk_channel_depth_delta += other.risk_channel_depth_delta;
        self.decision_channel_depth_delta += other.decision_channel_depth_delta;
        self.lifecycle_channel_depth_delta += other.lifecycle_channel_depth_delta;
        self.command_channel_depth_delta += other.command_channel_depth_delta;
        self.journal_channel_depth_delta += other.journal_channel_depth_delta;

        for (k, v) in &other.per_symbol_ticks {
            *self.per_symbol_ticks.entry(k.clone()).or_insert(0) += v;
        }
        for (k, v) in &other.per_symbol_features {
            *self.per_symbol_features.entry(k.clone()).or_insert(0) += v;
        }
        for (k, v) in &other.per_symbol_intents_approved {
            *self.per_symbol_intents_approved.entry(k.clone()).or_insert(0) += v;
        }
        for (k, v) in &other.per_symbol_intents_rejected {
            *self.per_symbol_intents_rejected.entry(k.clone()).or_insert(0) += v;
        }
        for (k, v) in &other.per_symbol_ticks_skipped {
            *self.per_symbol_ticks_skipped.entry(k.clone()).or_insert(0) += v;
        }
    }
}

pub struct ThreadLocalMetrics {
    batch: MetricsBatch,
    tick_count: u64,
    last_flush: Instant,
    flush_interval: Duration,
    flush_tick_threshold: u64,
    batch_tx: Sender<MetricsBatch>,
}

impl ThreadLocalMetrics {
    pub fn new(batch_tx: Sender<MetricsBatch>, flush_interval: Duration, flush_tick_threshold: u64) -> Self {
        Self {
            batch: MetricsBatch::new(),
            tick_count: 0,
            last_flush: Instant::now(),
            flush_interval,
            flush_tick_threshold,
            batch_tx,
        }
    }

    /// Record a tick processed (this is the only atomic on the hot path)
    #[inline]
    pub fn record_tick(&self, global_metrics: &GlobalMetrics) {
        global_metrics.ticks_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Accumulate metrics locally (no atomics)
    #[inline]
    pub fn accumulate(&mut self, symbol: &str) {
        self.batch.ticks_processed += 1;
        self.batch.features_computed += 1;
        self.tick_count += 1;

        *self.batch.per_symbol_ticks.entry(symbol.to_string()).or_insert(0) += 1;
        *self.batch.per_symbol_features.entry(symbol.to_string()).or_insert(0) += 1;

        // Check if we should flush
        if self.should_flush() {
            self.flush();
        }
    }

    /// Record feature channel depth change
    #[inline]
    pub fn record_feature_channel_depth(&mut self, delta: i64) {
        self.batch.feature_channel_depth_delta += delta;
    }

    /// Record risk channel depth change
    #[inline]
    pub fn record_risk_channel_depth(&mut self, delta: i64) {
        self.batch.risk_channel_depth_delta += delta;
    }

    /// Record intents generated
    #[inline]
    pub fn record_intent_generated(&mut self) {
        self.batch.intents_generated += 1;
    }

    /// Record dropped intents
    #[inline]
    pub fn record_dropped_intent(&mut self) {
        self.batch.dropped_intents += 1;
    }

    /// Record model fallback activation
    #[inline]
    pub fn record_model_fallback(&mut self) {
        self.batch.model_fallback_activations += 1;
    }

    /// Record feed gap
    #[inline]
    pub fn record_feed_gap(&mut self) {
        self.batch.feed_gaps += 1;
    }

    /// Record latency in a histogram
    #[inline]
    fn record_latency(latency_ns: u64, histogram: &mut [u64; 6]) {
        for (i, &threshold) in LATENCY_BUCKETS.iter().enumerate() {
            if latency_ns < threshold {
                histogram[i] += 1;
                return;
            }
        }
        histogram[5] += 1;
    }

    /// Record tick-to-intent latency
    #[inline]
    pub fn record_tick_to_intent_latency(&mut self, latency_ns: u64) {
        Self::record_latency(latency_ns, &mut self.batch.tick_to_intent_latency);
    }

    /// Record feed latency
    #[inline]
    pub fn record_feed_latency(&mut self, latency_ns: u64) {
        Self::record_latency(latency_ns, &mut self.batch.feed_latency);
    }

    /// Record model divergence magnitude (absolute difference between active and shadow predictions).
    /// The divergence value is scaled to u64 before bucketing: multiply by 1_000_000 so that
    /// a divergence of 0.001 maps to 1000, 0.01 to 10_000, 0.1 to 100_000, etc.
    #[inline]
    pub fn record_model_divergence(&mut self, divergence: f32) {
        let scaled = (divergence.abs() * 1_000_000.0) as u64;
        Self::record_latency(scaled, &mut self.batch.model_divergence);
    }

    /// Check if we should flush based on tick count or time
    #[inline]
    fn should_flush(&self) -> bool {
        self.tick_count >= self.flush_tick_threshold || 
            self.last_flush.elapsed() >= self.flush_interval
    }

    /// Flush the current batch to the aggregator channel
    #[inline]
    pub fn flush(&mut self) {
        if self.batch.ticks_processed == 0 && self.batch.features_computed == 0 {
            return;
        }

        let batch = std::mem::replace(&mut self.batch, MetricsBatch::new());
        let _ = self.batch_tx.send(batch);

        self.tick_count = 0;
        self.last_flush = Instant::now();
    }
}

impl Drop for ThreadLocalMetrics {
    fn drop(&mut self) {
        self.flush();
    }
}

/// Background thread that aggregates metrics batches from thread-local accumulators
/// and updates the global metrics atomically.
pub struct MetricsAggregator {
    batch_rx: Receiver<MetricsBatch>,
    global_metrics: Arc<GlobalMetrics>,
    shutdown_rx: Receiver<()>,
}

impl MetricsAggregator {
    pub fn new(
        batch_rx: Receiver<MetricsBatch>,
        global_metrics: Arc<GlobalMetrics>,
        shutdown_rx: Receiver<()>,
    ) -> Self {
        Self {
            batch_rx,
            global_metrics,
            shutdown_rx,
        }
    }

    /// Run the aggregator loop
    pub fn run(&self) {
        tracing::info!("Metrics aggregator started");
        
        loop {
            // Use a short timeout to allow periodic flushes even without batches
            match self.batch_rx.recv_timeout(Duration::from_millis(100)) {
                Ok(batch) => {
                    self.apply_batch(&batch);
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    // Check for shutdown signal
                    if self.shutdown_rx.try_recv().is_ok() {
                        tracing::info!("Metrics aggregator received shutdown signal");
                        break;
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    tracing::info!("Metrics aggregator channel disconnected");
                    break;
                }
            }
        }

        // Drain any remaining batches before shutdown
        while let Ok(batch) = self.batch_rx.try_recv() {
            self.apply_batch(&batch);
        }

        tracing::info!("Metrics aggregator stopped");
    }

    /// Apply a batch of metrics to the global metrics
    fn apply_batch(&self, batch: &MetricsBatch) {
        let m = &self.global_metrics;

        m.features_computed.fetch_add(batch.features_computed, Ordering::Relaxed);
        m.intents_generated.fetch_add(batch.intents_generated, Ordering::Relaxed);
        m.intents_approved.fetch_add(batch.intents_approved, Ordering::Relaxed);
        m.intents_rejected.fetch_add(batch.intents_rejected, Ordering::Relaxed);
        m.dropped_intents.fetch_add(batch.dropped_intents, Ordering::Relaxed);
        m.stale_predictions.fetch_add(batch.stale_predictions, Ordering::Relaxed);
        m.model_fallback_activations.fetch_add(batch.model_fallback_activations, Ordering::Relaxed);
        m.ticks_skipped.fetch_add(batch.ticks_skipped, Ordering::Relaxed);
        m.orders_submitted.fetch_add(batch.orders_submitted, Ordering::Relaxed);
        m.orders_filled.fetch_add(batch.orders_filled, Ordering::Relaxed);
        m.orders_cancelled.fetch_add(batch.orders_cancelled, Ordering::Relaxed);
        m.orders_rejected.fetch_add(batch.orders_rejected, Ordering::Relaxed);
        m.orders_lifecycle_events.fetch_add(batch.orders_lifecycle_events, Ordering::Relaxed);
        m.circuit_breaker_trips.fetch_add(batch.circuit_breaker_trips, Ordering::Relaxed);
        m.kill_switch_activations.fetch_add(batch.kill_switch_activations, Ordering::Relaxed);
        m.config_reloads.fetch_add(batch.config_reloads, Ordering::Relaxed);
        m.journal_writes.fetch_add(batch.journal_writes, Ordering::Relaxed);
        m.heartbeat_misses.fetch_add(batch.heartbeat_misses, Ordering::Relaxed);
        m.errors.fetch_add(batch.errors, Ordering::Relaxed);
        m.feed_gaps.fetch_add(batch.feed_gaps, Ordering::Relaxed);

        // Apply histogram buckets
        for (i, &v) in batch.tick_to_intent_latency.iter().enumerate() {
            if v > 0 {
                m.tick_to_intent_latency.buckets[i].fetch_add(v, Ordering::Relaxed);
            }
        }
        for (i, &v) in batch.risk_check_latency.iter().enumerate() {
            if v > 0 {
                m.risk_check_latency.buckets[i].fetch_add(v, Ordering::Relaxed);
            }
        }
        for (i, &v) in batch.journal_flush_latency.iter().enumerate() {
            if v > 0 {
                m.journal_flush_latency.buckets[i].fetch_add(v, Ordering::Relaxed);
            }
        }
        for (i, &v) in batch.broker_send_latency.iter().enumerate() {
            if v > 0 {
                m.broker_send_latency.buckets[i].fetch_add(v, Ordering::Relaxed);
            }
        }
        for (i, &v) in batch.feed_latency.iter().enumerate() {
            if v > 0 {
                m.feed_latency.buckets[i].fetch_add(v, Ordering::Relaxed);
            }
        }
        for (i, &v) in batch.broker_round_trip_latency.iter().enumerate() {
            if v > 0 {
                m.broker_round_trip_latency.buckets[i].fetch_add(v, Ordering::Relaxed);
            }
        }
        for (i, &v) in batch.decision_latency.iter().enumerate() {
            if v > 0 {
                m.decision_latency.buckets[i].fetch_add(v, Ordering::Relaxed);
            }
        }
        for (i, &v) in batch.model_divergence.iter().enumerate() {
            if v > 0 {
                m.model_divergence.buckets[i].fetch_add(v, Ordering::Relaxed);
            }
        }

        // Apply channel depth deltas
        if batch.feature_channel_depth_delta != 0 {
            m.feature_channel_depth.fetch_add(batch.feature_channel_depth_delta, Ordering::Relaxed);
        }
        if batch.risk_channel_depth_delta != 0 {
            m.risk_channel_depth.fetch_add(batch.risk_channel_depth_delta, Ordering::Relaxed);
        }
        if batch.decision_channel_depth_delta != 0 {
            m.decision_channel_depth.fetch_add(batch.decision_channel_depth_delta, Ordering::Relaxed);
        }
        if batch.lifecycle_channel_depth_delta != 0 {
            m.lifecycle_channel_depth.fetch_add(batch.lifecycle_channel_depth_delta, Ordering::Relaxed);
        }
        if batch.command_channel_depth_delta != 0 {
            m.command_channel_depth.fetch_add(batch.command_channel_depth_delta, Ordering::Relaxed);
        }
        if batch.journal_channel_depth_delta != 0 {
            m.journal_channel_depth.fetch_add(batch.journal_channel_depth_delta, Ordering::Relaxed);
        }

        // Apply per-symbol counters
        if !batch.per_symbol_ticks.is_empty() {
            let mut map = m.per_symbol_ticks.lock();
            for (symbol, count) in &batch.per_symbol_ticks {
                map.entry(symbol.clone())
                    .or_insert_with(|| AtomicU64::new(0))
                    .fetch_add(*count, Ordering::Relaxed);
            }
        }
        if !batch.per_symbol_features.is_empty() {
            let mut map = m.per_symbol_features.lock();
            for (symbol, count) in &batch.per_symbol_features {
                map.entry(symbol.clone())
                    .or_insert_with(|| AtomicU64::new(0))
                    .fetch_add(*count, Ordering::Relaxed);
            }
        }
        if !batch.per_symbol_intents_approved.is_empty() {
            let mut map = m.per_symbol_intents_approved.lock();
            for (symbol, count) in &batch.per_symbol_intents_approved {
                map.entry(symbol.clone())
                    .or_insert_with(|| AtomicU64::new(0))
                    .fetch_add(*count, Ordering::Relaxed);
            }
        }
        if !batch.per_symbol_intents_rejected.is_empty() {
            let mut map = m.per_symbol_intents_rejected.lock();
            for (symbol, count) in &batch.per_symbol_intents_rejected {
                map.entry(symbol.clone())
                    .or_insert_with(|| AtomicU64::new(0))
                    .fetch_add(*count, Ordering::Relaxed);
            }
        }
        if !batch.per_symbol_ticks_skipped.is_empty() {
            let mut map = m.per_symbol_ticks_skipped.lock();
            for (symbol, count) in &batch.per_symbol_ticks_skipped {
                map.entry(symbol.clone())
                    .or_insert_with(|| AtomicU64::new(0))
                    .fetch_add(*count, Ordering::Relaxed);
            }
        }
    }
}

pub struct GlobalMetrics {
    pub ticks_processed: CachePaddedAtomicU64,
    pub features_computed: CachePaddedAtomicU64,
    pub inferences_run: CachePaddedAtomicU64,
    pub intents_generated: CachePaddedAtomicU64,
    pub intents_approved: CachePaddedAtomicU64,
    pub intents_rejected: CachePaddedAtomicU64,
    pub dropped_intents: CachePaddedAtomicU64,
    pub stale_predictions: CachePaddedAtomicU64,
    pub model_fallback_activations: CachePaddedAtomicU64,
    pub ticks_skipped: CachePaddedAtomicU64,
    pub orders_submitted: CachePaddedAtomicU64,
    pub orders_filled: CachePaddedAtomicU64,
    pub orders_cancelled: CachePaddedAtomicU64,
    pub orders_rejected: CachePaddedAtomicU64,
    pub orders_lifecycle_events: CachePaddedAtomicU64,
    pub circuit_breaker_trips: CachePaddedAtomicU64,
    pub kill_switch_activations: CachePaddedAtomicU64,
    pub config_reloads: CachePaddedAtomicU64,
    pub journal_writes: CachePaddedAtomicU64,
    pub heartbeat_misses: CachePaddedAtomicU64,
    pub errors: CachePaddedAtomicU64,
    pub feed_gaps: CachePaddedAtomicU64,
    // Latency histograms
    pub tick_to_intent_latency: LatencyHistogram,
    pub risk_check_latency: LatencyHistogram,
    pub journal_flush_latency: LatencyHistogram,
    pub broker_send_latency: LatencyHistogram,
    pub feed_latency: LatencyHistogram,
    pub broker_round_trip_latency: LatencyHistogram,
    pub decision_latency: LatencyHistogram,
    /// Histogram of prediction divergence between active and shadow models
    pub model_divergence: LatencyHistogram,
    // Channel depth gauges (approximate, since crossbeam doesn't expose len())
    pub feature_channel_depth: AtomicI64,
    pub risk_channel_depth: AtomicI64,
    pub decision_channel_depth: AtomicI64,
    pub lifecycle_channel_depth: AtomicI64,
    pub command_channel_depth: AtomicI64,
    pub journal_channel_depth: AtomicI64,
    // Per-symbol counters
    pub per_symbol_ticks: Mutex<HashMap<String, AtomicU64>>,
    pub per_symbol_features: Mutex<HashMap<String, AtomicU64>>,
    pub per_symbol_intents_approved: Mutex<HashMap<String, AtomicU64>>,
    pub per_symbol_intents_rejected: Mutex<HashMap<String, AtomicU64>>,
    pub per_symbol_ticks_skipped: Mutex<HashMap<String, AtomicU64>>,
}

impl GlobalMetrics {
    pub fn new() -> Self {
        Self {
            ticks_processed: CachePaddedAtomicU64::new(0),
            features_computed: CachePaddedAtomicU64::new(0),
            inferences_run: CachePaddedAtomicU64::new(0),
            intents_generated: CachePaddedAtomicU64::new(0),
            intents_approved: CachePaddedAtomicU64::new(0),
            intents_rejected: CachePaddedAtomicU64::new(0),
            dropped_intents: CachePaddedAtomicU64::new(0),
            stale_predictions: CachePaddedAtomicU64::new(0),
            model_fallback_activations: CachePaddedAtomicU64::new(0),
            ticks_skipped: CachePaddedAtomicU64::new(0),
            orders_submitted: CachePaddedAtomicU64::new(0),
            orders_filled: CachePaddedAtomicU64::new(0),
            orders_cancelled: CachePaddedAtomicU64::new(0),
            orders_rejected: CachePaddedAtomicU64::new(0),
            orders_lifecycle_events: CachePaddedAtomicU64::new(0),
            circuit_breaker_trips: CachePaddedAtomicU64::new(0),
            kill_switch_activations: CachePaddedAtomicU64::new(0),
            config_reloads: CachePaddedAtomicU64::new(0),
            journal_writes: CachePaddedAtomicU64::new(0),
            heartbeat_misses: CachePaddedAtomicU64::new(0),
            errors: CachePaddedAtomicU64::new(0),
            feed_gaps: CachePaddedAtomicU64::new(0),
            tick_to_intent_latency: LatencyHistogram::new(),
            risk_check_latency: LatencyHistogram::new(),
            journal_flush_latency: LatencyHistogram::new(),
            broker_send_latency: LatencyHistogram::new(),
            feed_latency: LatencyHistogram::new(),
            broker_round_trip_latency: LatencyHistogram::new(),
            decision_latency: LatencyHistogram::new(),
            model_divergence: LatencyHistogram::new(),
            feature_channel_depth: AtomicI64::new(0),
            risk_channel_depth: AtomicI64::new(0),
            decision_channel_depth: AtomicI64::new(0),
            lifecycle_channel_depth: AtomicI64::new(0),
            command_channel_depth: AtomicI64::new(0),
            journal_channel_depth: AtomicI64::new(0),
            per_symbol_ticks: Mutex::new(HashMap::new()),
            per_symbol_features: Mutex::new(HashMap::new()),
            per_symbol_intents_approved: Mutex::new(HashMap::new()),
            per_symbol_intents_rejected: Mutex::new(HashMap::new()),
            per_symbol_ticks_skipped: Mutex::new(HashMap::new()),
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
        self.model_fallback_activations.store(0, Ordering::Relaxed);
        self.ticks_skipped.store(0, Ordering::Relaxed);
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
        self.feed_gaps.store(0, Ordering::Relaxed);
        self.tick_to_intent_latency.reset();
        self.risk_check_latency.reset();
        self.journal_flush_latency.reset();
        self.broker_send_latency.reset();
        self.feed_latency.reset();
        self.broker_round_trip_latency.reset();
        self.decision_latency.reset();
        self.model_divergence.reset();
        self.feature_channel_depth.store(0, Ordering::Relaxed);
        self.risk_channel_depth.store(0, Ordering::Relaxed);
        self.decision_channel_depth.store(0, Ordering::Relaxed);
        self.lifecycle_channel_depth.store(0, Ordering::Relaxed);
        self.command_channel_depth.store(0, Ordering::Relaxed);
        self.journal_channel_depth.store(0, Ordering::Relaxed);
        self.per_symbol_ticks.lock().clear();
        self.per_symbol_features.lock().clear();
        self.per_symbol_intents_approved.lock().clear();
        self.per_symbol_intents_rejected.lock().clear();
        self.per_symbol_ticks_skipped.lock().clear();
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let per_symbol_ticks = {
            let map = self.per_symbol_ticks.lock();
            map.iter()
                .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
                .collect()
        };
        let per_symbol_features = {
            let map = self.per_symbol_features.lock();
            map.iter()
                .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
                .collect()
        };
        let per_symbol_intents_approved = {
            let map = self.per_symbol_intents_approved.lock();
            map.iter()
                .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
                .collect()
        };
        let per_symbol_intents_rejected = {
            let map = self.per_symbol_intents_rejected.lock();
            map.iter()
                .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
                .collect()
        };
        let per_symbol_ticks_skipped = {
            let map = self.per_symbol_ticks_skipped.lock();
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
            model_fallback_activations: self.model_fallback_activations.load(Ordering::Relaxed),
            ticks_skipped: self.ticks_skipped.load(Ordering::Relaxed),
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
            feed_gaps: self.feed_gaps.load(Ordering::Relaxed),
            tick_to_intent_latency: self.tick_to_intent_latency.snapshot(),
            risk_check_latency: self.risk_check_latency.snapshot(),
            journal_flush_latency: self.journal_flush_latency.snapshot(),
            broker_send_latency: self.broker_send_latency.snapshot(),
            feed_latency: self.feed_latency.snapshot(),
            broker_round_trip_latency: self.broker_round_trip_latency.snapshot(),
            decision_latency: self.decision_latency.snapshot(),
            model_divergence: self.model_divergence.snapshot(),
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
            per_symbol_ticks_skipped,
        }
    }

    pub fn increment_per_symbol_tick(&self, symbol: &str) {
        let mut map = self.per_symbol_ticks.lock();
        map.entry(symbol.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_per_symbol_feature(&self, symbol: &str) {
        let mut map = self.per_symbol_features.lock();
        map.entry(symbol.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_per_symbol_intent_approved(&self, symbol: &str) {
        let mut map = self.per_symbol_intents_approved.lock();
        map.entry(symbol.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_per_symbol_intent_rejected(&self, symbol: &str) {
        let mut map = self.per_symbol_intents_rejected.lock();
        map.entry(symbol.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_per_symbol_tick_skipped(&self, symbol: &str) {
        let mut map = self.per_symbol_ticks_skipped.lock();
        map.entry(symbol.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Spawn a background metrics aggregator thread that receives batches from
    /// thread-local accumulators and updates global metrics atomically.
    /// Returns a tuple of (batch_tx, shutdown_tx, thread_handle).
    pub fn spawn_aggregator(
        self: &Arc<Self>,
        core_id: usize,
        flush_interval: Duration,
        flush_tick_threshold: u64,
        channel_capacity: usize,
    ) -> (Sender<MetricsBatch>, Sender<()>, std::thread::JoinHandle<()>) {
        let (batch_tx, batch_rx) = bounded::<MetricsBatch>(channel_capacity);
        let (shutdown_tx, shutdown_rx) = bounded::<()>(1);

        let metrics = Arc::clone(self);
        let aggregator = MetricsAggregator::new(batch_rx, metrics, shutdown_rx);

        let handle = spawn_pinned(
            "metrics-aggregator",
            core_id,
            ThreadPriority::Normal,
            move || {
                aggregator.run();
            },
        ).expect("spawn_pinned failed for metrics aggregator");

        (batch_tx, shutdown_tx, handle)
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
    pub model_fallback_activations: u64,
    pub ticks_skipped: u64,
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
    pub feed_gaps: u64,
    // Latency histograms (buckets: <1us, <10us, <100us, <1ms, <10ms, >10ms)
    pub tick_to_intent_latency: [u64; 6],
    pub risk_check_latency: [u64; 6],
    pub journal_flush_latency: [u64; 6],
    pub broker_send_latency: [u64; 6],
    pub feed_latency: [u64; 6],
    pub broker_round_trip_latency: [u64; 6],
    pub decision_latency: [u64; 6],
    // Model divergence histogram buckets
    pub model_divergence: [u64; 6],
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
    pub per_symbol_ticks_skipped: std::collections::HashMap<String, u64>,
}

impl Default for GlobalMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

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

    /// Micro-benchmark style contention test:
    /// - Two threads hammer two hot counters.
    /// - With cache-line padding, counters should not false-share a cache line.
    ///
    /// For Linux perf validation (acceptance criteria), run:
    /// `perf stat -e cache-misses,cache-references cargo test -p core metrics_false_sharing_hammer -- --nocapture`
    /// (Adjust `-p` package name if needed by workspace manifest.)
    #[test]
    fn metrics_false_sharing_hammer() {
        let metrics = Arc::new(GlobalMetrics::new());
        let iterations = 2_000_000u64;

        let m1 = Arc::clone(&metrics);
        let t1 = thread::spawn(move || {
            for _ in 0..iterations {
                m1.ticks_processed.fetch_add(1, Ordering::Relaxed);
            }
        });

        let m2 = Arc::clone(&metrics);
        let t2 = thread::spawn(move || {
            for _ in 0..iterations {
                m2.features_computed.fetch_add(1, Ordering::Relaxed);
            }
        });

        t1.join().expect("thread 1 join failed");
        t2.join().expect("thread 2 join failed");

        assert_eq!(metrics.ticks_processed.load(Ordering::Relaxed), iterations);
        assert_eq!(metrics.features_computed.load(Ordering::Relaxed), iterations);
    }

    #[test]
    fn test_metrics_batch_merge() {
        let mut batch1 = MetricsBatch::new();
        batch1.ticks_processed = 100;
        batch1.features_computed = 100;
        batch1.intents_generated = 50;
        batch1.per_symbol_ticks.insert("AAPL".to_string(), 50);
        batch1.per_symbol_features.insert("AAPL".to_string(), 50);

        let mut batch2 = MetricsBatch::new();
        batch2.ticks_processed = 50;
        batch2.features_computed = 50;
        batch2.intents_generated = 25;
        batch2.per_symbol_ticks.insert("AAPL".to_string(), 25);
        batch2.per_symbol_ticks.insert("TSLA".to_string(), 30);

        batch1.merge(&batch2);

        assert_eq!(batch1.ticks_processed, 150);
        assert_eq!(batch1.features_computed, 150);
        assert_eq!(batch1.intents_generated, 75);
        assert_eq!(batch1.per_symbol_ticks.get("AAPL"), Some(&75));
        assert_eq!(batch1.per_symbol_ticks.get("TSLA"), Some(&30));
    }

    #[test]
    fn test_thread_local_metrics_accumulation() {
        use crossbeam_channel::bounded;
        use std::time::Duration;

        let (tx, _rx) = bounded::<MetricsBatch>(100);
        let metrics = Arc::new(GlobalMetrics::new());
        
        let mut local = ThreadLocalMetrics::new(
            tx,
            Duration::from_secs(60), // Long interval so we don't auto-flush
            10_000, // High threshold so we don't auto-flush
        );

        // Accumulate some metrics
        local.accumulate("AAPL");
        local.accumulate("AAPL");
        local.accumulate("TSLA");

        // Manually flush
        local.flush();

        // Check that the batch was sent (we can't easily check the channel contents
        // without draining it, but we can verify the local state was reset)
        assert_eq!(local.batch.ticks_processed, 0);
        assert_eq!(local.batch.features_computed, 0);
    }

    #[test]
    fn test_thread_local_metrics_tick_threshold_flush() {
        use crossbeam_channel::bounded;
        use std::time::Duration;

        let (tx, rx) = bounded::<MetricsBatch>(100);
        let metrics = Arc::new(GlobalMetrics::new());
        
        let mut local = ThreadLocalMetrics::new(
            tx,
            Duration::from_secs(60), // Long interval
            3, // Low threshold - flush after 3 ticks
        );

        // Accumulate exactly at threshold
        local.accumulate("AAPL");
        local.accumulate("AAPL");
        // Third accumulate should trigger flush
        local.accumulate("AAPL");

        // Should have received a batch
        let batch = rx.recv_timeout(Duration::from_millis(100)).unwrap();
        assert_eq!(batch.ticks_processed, 3);
        assert_eq!(batch.features_computed, 3);
    }

    #[test]
    fn test_thread_local_metrics_drop_flushes() {
        use crossbeam_channel::bounded;
        use std::time::Duration;

        let (tx, rx) = bounded::<MetricsBatch>(100);
        let metrics = Arc::new(GlobalMetrics::new());
        
        {
            let mut local = ThreadLocalMetrics::new(
                tx,
                Duration::from_secs(60),
                10_000,
            );
            local.accumulate("AAPL");
            local.accumulate("AAPL");
            // Drop local here - should flush
        }

        // Should have received a batch on drop
        let batch = rx.recv_timeout(Duration::from_millis(100)).unwrap();
        assert_eq!(batch.ticks_processed, 2);
        assert_eq!(batch.features_computed, 2);
    }

    #[test]
    fn test_metrics_aggregator_apply_batch() {
        use crossbeam_channel::bounded;
        use std::time::Duration;

        let (tx, rx) = bounded::<MetricsBatch>(100);
        let (shutdown_tx, shutdown_rx) = bounded::<()>(1);
        let metrics = Arc::new(GlobalMetrics::new());
        
        let aggregator = MetricsAggregator::new(rx, Arc::clone(&metrics), shutdown_rx);

        // Create a test batch
        let mut batch = MetricsBatch::new();
        batch.ticks_processed = 0; // Not used by aggregator (hot path only)
        batch.features_computed = 100;
        batch.intents_generated = 50;
        batch.feed_gaps = 5;
        batch.per_symbol_ticks.insert("AAPL".to_string(), 75);
        batch.per_symbol_features.insert("AAPL".to_string(), 75);

        // Apply the batch
        aggregator.apply_batch(&batch);

        // Verify metrics were updated
        assert_eq!(metrics.features_computed.load(Ordering::Relaxed), 100);
        assert_eq!(metrics.intents_generated.load(Ordering::Relaxed), 50);
        assert_eq!(metrics.feed_gaps.load(Ordering::Relaxed), 5);

        // Verify per-symbol counters
        let snap = metrics.snapshot();
        assert_eq!(snap.per_symbol_ticks.get("AAPL"), Some(&75));
        assert_eq!(snap.per_symbol_features.get("AAPL"), Some(&75));

        // Cleanup
        drop(tx);
        drop(shutdown_tx);
    }
}
