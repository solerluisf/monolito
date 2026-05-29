use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use parking_lot::Mutex;

use crate::clock::{Clock, WallClock, wall_time_ns};

pub struct KillSwitch {
    active: AtomicBool,
    activated_at_ns: AtomicU64,
    activation_count: AtomicU64,
    open_orders: Arc<Mutex<HashSet<String>>>,
    activation_latency_ns: AtomicU64,  // Time from activate() call to store completion
    clock: Arc<dyn Clock>,
}

impl KillSwitch {
    pub fn new() -> Self {
        Self::with_clock(Arc::new(WallClock::new()))
    }

    pub fn with_clock(clock: Arc<dyn Clock>) -> Self {
        Self {
            active: AtomicBool::new(false),
            activated_at_ns: AtomicU64::new(0),
            activation_count: AtomicU64::new(0),
            open_orders: Arc::new(Mutex::new(HashSet::new())),
            activation_latency_ns: AtomicU64::new(0),
            clock,
        }
    }

    /// Activates the kill switch and returns the number of open orders.
    /// Measures the time taken for the atomic store operation.
    pub fn activate(&self) -> usize {
        let start = std::time::Instant::now();
        
        let order_count = {
            let orders = self.open_orders.lock();
            orders.len()
        };

        if !self.active.load(Ordering::Relaxed) {
            let now = self.clock.now_ns();
            self.activated_at_ns.store(now, Ordering::SeqCst);
            self.activation_count.fetch_add(1, Ordering::Relaxed);
        }
        self.active.store(true, Ordering::SeqCst);
        
        // Measure activation latency
        let latency = start.elapsed().as_nanos() as u64;
        self.activation_latency_ns.store(latency, Ordering::Relaxed);

        if order_count > 0 {
            tracing::error!("KILL SWITCH ACTIVATED - {} open orders tracked for cancellation", order_count);
        }

        order_count
    }

    pub fn clear(&self) {
        self.active.store(false, Ordering::SeqCst);
        tracing::info!("Kill switch deactivated - new orders will be accepted");
    }

    pub fn activation_latency_ns(&self) -> u64 {
        self.activation_latency_ns.load(Ordering::Relaxed)
    }

    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub fn activated_at_ns(&self) -> u64 {
        self.activated_at_ns.load(Ordering::Relaxed)
    }

    pub fn activation_count(&self) -> u64 {
        self.activation_count.load(Ordering::Relaxed)
    }

    pub fn track_open_order(&self, order_id: &str) -> bool {
        let mut orders = self.open_orders.lock();
        let added = orders.insert(order_id.to_string());
        if added {
            tracing::debug!("Tracking open order: {}", order_id);
        }
        added
    }

    pub fn remove_open_order(&self, order_id: &str) -> bool {
        let mut orders = self.open_orders.lock();
        let removed = orders.remove(order_id);
        if removed {
            tracing::debug!("Order no longer tracked: {}", order_id);
        }
        removed
    }

    pub fn open_order_count(&self) -> usize {
        self.open_orders.lock().len()
    }

    pub fn get_open_orders(&self) -> Vec<String> {
        self.open_orders.lock().iter().cloned().collect()
    }

    pub fn is_order_tracked(&self, order_id: &str) -> bool {
        self.open_orders.lock().contains(order_id)
    }
}

impl Default for KillSwitch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::TestClock;

    #[test]
    fn test_kill_switch_initial_state() {
        let ks = KillSwitch::new();
        assert!(!ks.is_active());
        assert_eq!(ks.activation_count(), 0);
        assert_eq!(ks.open_order_count(), 0);
    }

    #[test]
    fn test_kill_switch_activate() {
        let ks = KillSwitch::new();
        ks.activate();
        assert!(ks.is_active());
        assert_eq!(ks.activation_count(), 1);
        assert!(ks.activated_at_ns() > 0);
    }

    #[test]
    fn test_kill_switch_clear() {
        let ks = KillSwitch::new();
        ks.activate();
        ks.clear();
        assert!(!ks.is_active());
    }

    #[test]
    fn test_kill_switch_tracks_orders() {
        let ks = KillSwitch::new();
        assert!(ks.track_open_order("order-1"));
        assert!(ks.track_open_order("order-2"));
        assert!(!ks.track_open_order("order-1"));
        assert_eq!(ks.open_order_count(), 2);
        assert!(ks.is_order_tracked("order-1"));
        assert!(ks.is_order_tracked("order-2"));
        assert!(!ks.is_order_tracked("order-3"));
    }

    #[test]
    fn test_kill_switch_removes_orders() {
        let ks = KillSwitch::new();
        ks.track_open_order("order-1");
        assert!(ks.remove_open_order("order-1"));
        assert!(!ks.remove_open_order("order-1"));
        assert_eq!(ks.open_order_count(), 0);
    }

    #[test]
    fn test_kill_switch_activate_returns_order_count() {
        let ks = KillSwitch::new();
        ks.track_open_order("order-1");
        ks.track_open_order("order-2");
        let count = ks.activate();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_kill_switch_with_custom_clock() {
        let clock = Arc::new(TestClock::new(1_000_000_000_000));
        let ks = KillSwitch::with_clock(clock);
        ks.activate();
        assert!(ks.is_active());
        assert_eq!(ks.activated_at_ns(), 1_000_000_000_000);
    }

    /// Benchmark-style test measuring KillSwitch activation latency under concurrent reads.
    /// Spawns 8 reader threads hammering is_active() while the main thread triggers activate().
    #[test]
    fn test_kill_switch_latency_under_contention() {
        use std::time::{Duration, Instant};
        use std::sync::atomic::AtomicBool;

        let ks = Arc::new(KillSwitch::new());
        let running = Arc::new(AtomicBool::new(true));
        let mut handles = vec![];

        for _ in 0..8 {
            let ks_clone = Arc::clone(&ks);
            let r_clone = Arc::clone(&running);
            handles.push(std::thread::spawn(move || {
                while r_clone.load(Ordering::Relaxed) {
                    let _ = ks_clone.is_active();
                    std::thread::yield_now();
                }
            }));
        }

        // Warm-up
        std::thread::sleep(Duration::from_millis(10));

        let before = Instant::now();
        ks.activate();
        let after = Instant::now();

        let latency = after.duration_since(before);

        // Stop readers
        running.store(false, Ordering::Relaxed);
        for h in handles {
            let _ = h.join();
        }

        // Assert reasonable latency (should be < 1us on modern hardware, but we allow 100us in CI)
        assert!(
            latency < Duration::from_micros(100),
            "KillSwitch activation latency too high: {:?}",
            latency
        );

        // Also verify all threads eventually see it
        assert!(ks.is_active());
    }
}
