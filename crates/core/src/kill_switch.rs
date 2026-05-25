use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub struct KillSwitch {
    active: AtomicBool,
    activated_at_ns: AtomicU64,
    activation_count: AtomicU64,
    open_orders: Arc<Mutex<HashSet<String>>>,
    activation_latency_ns: AtomicU64,  // Time from activate() call to store completion
}

impl KillSwitch {
    pub fn new() -> Self {
        Self {
            active: AtomicBool::new(false),
            activated_at_ns: AtomicU64::new(0),
            activation_count: AtomicU64::new(0),
            open_orders: Arc::new(Mutex::new(HashSet::new())),
            activation_latency_ns: AtomicU64::new(0),
        }
    }

    /// Activates the kill switch and returns the number of open orders.
    /// Measures the time taken for the atomic store operation.
    pub fn activate(&self) -> usize {
        let start = std::time::Instant::now();
        
        let order_count = {
            let orders = self.open_orders.lock().unwrap();
            orders.len()
        };

        if !self.active.load(Ordering::Relaxed) {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
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
        let mut orders = self.open_orders.lock().unwrap();
        let added = orders.insert(order_id.to_string());
        if added {
            tracing::debug!("Tracking open order: {}", order_id);
        }
        added
    }

    pub fn remove_open_order(&self, order_id: &str) -> bool {
        let mut orders = self.open_orders.lock().unwrap();
        let removed = orders.remove(order_id);
        if removed {
            tracing::debug!("Order no longer tracked: {}", order_id);
        }
        removed
    }

    pub fn open_order_count(&self) -> usize {
        self.open_orders.lock().unwrap().len()
    }

    pub fn get_open_orders(&self) -> Vec<String> {
        self.open_orders.lock().unwrap().iter().cloned().collect()
    }

    pub fn is_order_tracked(&self, order_id: &str) -> bool {
        self.open_orders.lock().unwrap().contains(order_id)
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
}
