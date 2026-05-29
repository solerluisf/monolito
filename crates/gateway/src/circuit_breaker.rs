use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use parking_lot::Mutex;
use unified_trading_core::clock::wall_time_ns;

#[derive(Debug, Clone, Copy, PartialEq)]
enum State {
    Closed,
    Open,
    HalfOpen,
}

struct Inner {
    state: State,
    failures: u64,
    tripped_at_ns: u64,
}

pub struct CircuitBreaker {
    inner: Mutex<Inner>,
    failure_threshold: AtomicU64,
    cooldown_ms: AtomicU64,
    pub is_open: AtomicBool,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u64, cooldown_ms: u64) -> Self {
        Self {
            inner: Mutex::new(Inner {
                state: State::Closed,
                failures: 0,
                tripped_at_ns: 0,
            }),
            failure_threshold: AtomicU64::new(failure_threshold),
            cooldown_ms: AtomicU64::new(cooldown_ms),
            is_open: AtomicBool::new(false),
        }
    }

    fn failure_threshold_val(&self) -> u64 {
        self.failure_threshold.load(Ordering::Relaxed)
    }

    fn cooldown_ms_val(&self) -> u64 {
        self.cooldown_ms.load(Ordering::Relaxed)
    }

    pub fn record_success(&self) {
        let mut inner = self.inner.lock();
        let was_half_open = inner.state == State::HalfOpen;
        inner.failures = 0;
        inner.state = State::Closed;
        self.is_open.store(false, Ordering::SeqCst);

        if was_half_open {
            tracing::info!("Circuit breaker closed (probe succeeded)");
        }
    }

    pub fn record_failure(&self) {
        let mut inner = self.inner.lock();
        inner.failures += 1;
        let threshold = self.failure_threshold_val();

        if inner.failures >= threshold || inner.state == State::HalfOpen {
            let now = wall_time_ns();
            inner.tripped_at_ns = now;
            inner.state = State::Open;
            self.is_open.store(true, Ordering::SeqCst);
            tracing::error!("Circuit breaker opened after {} failures", inner.failures);
        } else {
            tracing::warn!("Circuit breaker failure {}/{}", inner.failures, threshold);
        }
    }

    pub fn trip(&self) {
        let mut inner = self.inner.lock();
        let now = wall_time_ns();
        inner.tripped_at_ns = now;
        inner.state = State::Open;
        inner.failures = self.failure_threshold_val();
        self.is_open.store(true, Ordering::SeqCst);
        tracing::error!("Circuit breaker manually tripped");
    }

    pub fn can_execute(&self) -> bool {
        let mut inner = self.inner.lock();

        match inner.state {
            State::Closed => true,
            State::Open => {
                let now = wall_time_ns();
                let elapsed_ms = now.saturating_sub(inner.tripped_at_ns) / 1_000_000;

                if elapsed_ms > self.cooldown_ms_val() {
                    inner.state = State::HalfOpen;
                    self.is_open.store(false, Ordering::SeqCst);
                    tracing::info!("Circuit breaker half-open (probing)");
                    true
                } else {
                    false
                }
            }
            State::HalfOpen => true,
        }
    }

    pub fn reset(&self) {
        let mut inner = self.inner.lock();
        inner.failures = 0;
        inner.state = State::Closed;
        self.is_open.store(false, Ordering::SeqCst);
    }

    pub fn set_failure_threshold(&self, threshold: u64) {
        self.failure_threshold.store(threshold, Ordering::Relaxed);
        tracing::info!(threshold = threshold, "Circuit breaker failure threshold updated");
    }

    pub fn set_cooldown_ms(&self, cooldown_ms: u64) {
        self.cooldown_ms.store(cooldown_ms, Ordering::Relaxed);
        tracing::info!(cooldown_ms = cooldown_ms, "Circuit breaker cooldown updated");
    }

    pub fn failure_count(&self) -> u64 {
        self.inner.lock().failures
    }

    pub fn state_name(&self) -> &'static str {
        let inner = self.inner.lock();
        match inner.state {
            State::Closed => "closed",
            State::Open => "open",
            State::HalfOpen => "half_open",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_breaker_initial_state() {
        let cb = CircuitBreaker::new(3, 1000);
        assert!(cb.can_execute());
        assert_eq!(cb.failure_count(), 0);
        assert_eq!(cb.state_name(), "closed");
    }

    #[test]
    fn test_circuit_breaker_records_failures() {
        let cb = CircuitBreaker::new(3, 1000);
        cb.record_failure();
        assert_eq!(cb.failure_count(), 1);
        cb.record_failure();
        assert_eq!(cb.failure_count(), 2);
    }

    #[test]
    fn test_circuit_breaker_trips_at_threshold() {
        let cb = CircuitBreaker::new(3, 1000);
        cb.record_failure();
        cb.record_failure();
        cb.record_failure();
        assert!(cb.is_open.load(Ordering::Relaxed));
        assert!(!cb.can_execute());
        assert_eq!(cb.state_name(), "open");
    }

    #[test]
    fn test_circuit_breaker_success_resets_count() {
        let cb = CircuitBreaker::new(3, 1000);
        cb.record_failure();
        cb.record_failure();
        cb.record_success();
        assert_eq!(cb.failure_count(), 0);
    }

    #[test]
    fn test_circuit_breaker_reset() {
        let cb = CircuitBreaker::new(3, 1000);
        cb.record_failure();
        cb.record_failure();
        cb.record_failure();
        cb.reset();
        assert!(!cb.is_open.load(Ordering::Relaxed));
        assert_eq!(cb.failure_count(), 0);
        assert_eq!(cb.state_name(), "closed");
    }

    #[test]
    fn test_circuit_breaker_half_open() {
        let cb = CircuitBreaker::new(3, 10);
        cb.record_failure();
        cb.record_failure();
        cb.record_failure();
        assert!(cb.is_open.load(Ordering::Relaxed));

        std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(cb.can_execute());
        assert_eq!(cb.state_name(), "half_open");

        cb.record_success();
        assert_eq!(cb.state_name(), "closed");
    }

    #[test]
    fn test_circuit_breaker_half_open_failure_reopens() {
        let cb = CircuitBreaker::new(3, 10);
        cb.record_failure();
        cb.record_failure();
        cb.record_failure();

        std::thread::sleep(std::time::Duration::from_millis(20));
        cb.can_execute();
        assert_eq!(cb.state_name(), "half_open");

        cb.record_failure();
        assert!(cb.is_open.load(Ordering::Relaxed));
        assert_eq!(cb.state_name(), "open");
    }
}
