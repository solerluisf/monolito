use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::clock::{Clock, WallClock};
use crate::kill_switch::KillSwitch;
use crate::metrics::GlobalMetrics;
use crate::threading::{spawn_pinned, ThreadPriority};

pub struct HeartbeatHandle {
    timestamp_ns: Arc<AtomicU64>,
    clock: Arc<dyn Clock>,
}

impl HeartbeatHandle {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            timestamp_ns: Arc::new(AtomicU64::new(0)),
            clock,
        }
    }

    /// Create a handle that shares the given timestamp atomic
    pub fn with_timestamp(timestamp_ns: Arc<AtomicU64>, clock: Arc<dyn Clock>) -> Self {
        Self {
            timestamp_ns,
            clock,
        }
    }

    pub fn pulse(&self) {
        let now = self.clock.now_ns();
        self.timestamp_ns.store(now, Ordering::Relaxed);
    }
}

pub struct ThreadHeartbeatMonitor {
    heartbeats: Arc<parking_lot::RwLock<HashMap<String, Arc<AtomicU64>>>>,
    handle: Option<thread::JoinHandle<()>>,
    running: Arc<AtomicBool>,
    asset_watchdog_timeout_ns: u64,
    clock: Arc<dyn Clock>,
}

impl ThreadHeartbeatMonitor {
    pub fn new(
        kill_switch: Arc<KillSwitch>,
        metrics: Arc<GlobalMetrics>,
        timeout_ns: u64,
        check_interval_ms: u64,
        core_id: usize,
    ) -> Self {
        Self::with_clock(
            kill_switch,
            metrics,
            timeout_ns,
            check_interval_ms,
            core_id,
            Arc::new(WallClock::new()),
        )
    }

    pub fn with_clock(
        kill_switch: Arc<KillSwitch>,
        metrics: Arc<GlobalMetrics>,
        timeout_ns: u64,
        check_interval_ms: u64,
        core_id: usize,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let heartbeats = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let running = Arc::new(AtomicBool::new(true));

        let hb = Arc::clone(&heartbeats);
        let ks = Arc::clone(&kill_switch);
        let m = Arc::clone(&metrics);
        let r = Arc::clone(&running);
        let c = Arc::clone(&clock);

        let asset_watchdog_timeout_ns = timeout_ns.saturating_div(2).max(1);

        let handle = spawn_pinned(
            "heartbeat-monitor",
            core_id,
            ThreadPriority::Normal,
            move || {
                Self::run_loop(
                    &hb,
                    &ks,
                    &m,
                    timeout_ns,
                    asset_watchdog_timeout_ns,
                    check_interval_ms,
                    &r,
                    &c,
                );
            },
        );

        Self {
            heartbeats,
            handle: Some(handle.expect("spawn_pinned failed")),
            running,
            asset_watchdog_timeout_ns,
            clock,
        }
    }

    fn run_loop(
        heartbeats: &parking_lot::RwLock<HashMap<String, Arc<AtomicU64>>>,
        kill_switch: &KillSwitch,
        metrics: &GlobalMetrics,
        timeout_ns: u64,
        asset_watchdog_timeout_ns: u64,
        check_interval_ms: u64,
        running: &AtomicBool,
        clock: &Arc<dyn Clock>,
    ) {
        let interval = Duration::from_millis(check_interval_ms);
        while running.load(Ordering::Relaxed) {
            thread::sleep(interval);

            if kill_switch.is_active() {
                continue;
            }

            let now = clock.now_ns();

            let map = heartbeats.read();
            for (name, ts) in map.iter() {
                let last = ts.load(Ordering::Relaxed);
                if last == 0 {
                    continue;
                }

                let stale_ns = now.saturating_sub(last);
                let watchdog_timeout_ns = if name.starts_with("asset-") {
                    asset_watchdog_timeout_ns
                } else {
                    timeout_ns
                };

                if stale_ns > watchdog_timeout_ns {
                    metrics.heartbeat_misses.fetch_add(1, Ordering::Relaxed);
                    if name.starts_with("asset-") {
                        tracing::warn!(
                            thread = %name,
                            stale_ns = stale_ns,
                            timeout_ns = watchdog_timeout_ns,
                            "Asset watchdog heartbeat stale, triggering KillSwitch"
                        );
                    } else {
                        tracing::warn!(
                            thread = %name,
                            stale_ns = stale_ns,
                            timeout_ns = watchdog_timeout_ns,
                            "Thread heartbeat stale, triggering KillSwitch"
                        );
                    }
                    kill_switch.activate();
                }
            }
        }
    }

    pub fn register_thread(&self, name: &str) -> HeartbeatHandle {
        let ts = Arc::new(AtomicU64::new(0));
        let mut map = self.heartbeats.write();
        map.insert(name.to_string(), Arc::clone(&ts));
        // Return handle that shares the same atomic
        HeartbeatHandle::with_timestamp(ts, Arc::clone(&self.clock))
    }

    pub fn heartbeats(&self) -> Arc<parking_lot::RwLock<HashMap<String, Arc<AtomicU64>>>> {
        Arc::clone(&self.heartbeats)
    }

    pub fn asset_watchdog_timeout_ns(&self) -> u64 {
        self.asset_watchdog_timeout_ns
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
    use crate::clock::TestClock;

    #[test]
    fn test_heartbeat_handle_pulse() {
        let clock: Arc<dyn Clock> = Arc::new(TestClock::new(1_000_000_000));
        let handle = HeartbeatHandle::new(Arc::clone(&clock));
        assert_eq!(handle.timestamp_ns.load(Ordering::Relaxed), 0);
        handle.pulse();
        assert_eq!(handle.timestamp_ns.load(Ordering::Relaxed), 1_000_000_000);
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
            100_000_000, // 100ms timeout
            10,          // check every 10ms
            0,
        );

        let handle = monitor.register_thread("stale_thread");
        handle.pulse();

        // Give monitor time to run multiple check cycles
        std::thread::sleep(Duration::from_millis(500));
        assert!(ks.is_active(), "Kill switch should be active after heartbeat timeout");
        assert!(metrics.heartbeat_misses.load(Ordering::Relaxed) > 0);

        let mut monitor = monitor;
        monitor.shutdown();
    }
}
