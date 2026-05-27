use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::kill_switch::KillSwitch;
use crate::metrics::GlobalMetrics;
use crate::threading::{spawn_pinned, ThreadPriority};

pub struct HeartbeatHandle {
    timestamp_ns: Arc<AtomicU64>,
}

impl HeartbeatHandle {
    pub fn pulse(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        self.timestamp_ns.store(now, Ordering::Relaxed);
    }
}

pub struct ThreadHeartbeatMonitor {
    heartbeats: Arc<parking_lot::RwLock<HashMap<String, Arc<AtomicU64>>>>,
    handle: Option<thread::JoinHandle<()>>,
    running: Arc<AtomicBool>,
}

impl ThreadHeartbeatMonitor {
    pub fn new(
        kill_switch: Arc<KillSwitch>,
        metrics: Arc<GlobalMetrics>,
        timeout_ns: u64,
        check_interval_ms: u64,
        core_id: usize,
    ) -> Self {
        let heartbeats = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let running = Arc::new(AtomicBool::new(true));

        let hb = Arc::clone(&heartbeats);
        let ks = Arc::clone(&kill_switch);
        let m = Arc::clone(&metrics);
        let r = Arc::clone(&running);

        let handle = spawn_pinned(
            "heartbeat-monitor",
            core_id,
            ThreadPriority::Normal,
            move || {
                Self::run_loop(&hb, &ks, &m, timeout_ns, check_interval_ms, &r);
            },
        );

        Self {
            heartbeats,
            handle: Some(handle.expect("spawn_pinned failed")),
            running,
        }
    }

    fn run_loop(
        heartbeats: &parking_lot::RwLock<HashMap<String, Arc<AtomicU64>>>,
        kill_switch: &KillSwitch,
        metrics: &GlobalMetrics,
        timeout_ns: u64,
        check_interval_ms: u64,
        running: &AtomicBool,
    ) {
        let interval = Duration::from_millis(check_interval_ms);
        while running.load(Ordering::Relaxed) {
            thread::sleep(interval);

            if kill_switch.is_active() {
                continue;
            }

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;

            let map = heartbeats.read();
            for (name, ts) in map.iter() {
                let last = ts.load(Ordering::Relaxed);
                if last > 0 && now.saturating_sub(last) > timeout_ns {
                    metrics.heartbeat_misses.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!("Thread {} heartbeat stale, triggering KillSwitch", name);
                    kill_switch.activate();
                }
            }
        }
    }

    pub fn register_thread(&self, name: &str) -> HeartbeatHandle {
        let ts = Arc::new(AtomicU64::new(0));
        let mut map = self.heartbeats.write();
        map.insert(name.to_string(), Arc::clone(&ts));
        HeartbeatHandle { timestamp_ns: ts }
    }

    pub fn heartbeats(&self) -> Arc<parking_lot::RwLock<HashMap<String, Arc<AtomicU64>>>> {
        Arc::clone(&self.heartbeats)
    }

    pub fn shutdown(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heartbeat_handle_pulse() {
        let ts = Arc::new(AtomicU64::new(0));
        let handle = HeartbeatHandle { timestamp_ns: Arc::clone(&ts) };
        assert_eq!(ts.load(Ordering::Relaxed), 0);
        handle.pulse();
        assert!(ts.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn test_monitor_register_thread() {
        let ks = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        let monitor = ThreadHeartbeatMonitor::new(ks, metrics, 2_000_000_000, 100, 0);

        let handle = monitor.register_thread("test_thread");
        handle.pulse();

        let mut monitor = monitor;
        monitor.shutdown();
    }

    #[test]
    fn test_monitor_stale_heartbeat_triggers_killswitch() {
        let ks = Arc::new(KillSwitch::new());
        let metrics = Arc::new(GlobalMetrics::new());
        let monitor = ThreadHeartbeatMonitor::new(
            Arc::clone(&ks),
            Arc::clone(&metrics),
            50_000_000, // 50ms timeout
            20,         // check every 20ms
            0,
        );

        let handle = monitor.register_thread("stale_thread");
        handle.pulse();

        std::thread::sleep(Duration::from_millis(100));
        assert!(ks.is_active());
        assert!(metrics.heartbeat_misses.load(Ordering::Relaxed) > 0);

        let mut monitor = monitor;
        monitor.shutdown();
    }
}
