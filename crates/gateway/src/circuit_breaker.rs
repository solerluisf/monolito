use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

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
    failure_threshold: u64,
    cooldown_ms: u64,
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
            failure_threshold,
            cooldown_ms,
            is_open: AtomicBool::new(false),
        }
    }

    pub fn record_success(&self) {
        let mut inner = self.inner.lock().unwrap();
        let was_half_open = inner.state == State::HalfOpen;
        inner.failures = 0;
        inner.state = State::Closed;
        self.is_open.store(false, Ordering::SeqCst);

        if was_half_open {
            tracing::info!("Circuit breaker closed (probe succeeded)");
        }
    }

    pub fn record_failure(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.failures += 1;

        if inner.failures >= self.failure_threshold || inner.state == State::HalfOpen {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            inner.tripped_at_ns = now;
            inner.state = State::Open;
            self.is_open.store(true, Ordering::SeqCst);
            tracing::error!("Circuit breaker opened after {} failures", inner.failures);
        } else {
            tracing::warn!("Circuit breaker failure {}/{}", inner.failures, self.failure_threshold);
        }
    }

    pub fn trip(&self) {
        let mut inner = self.inner.lock().unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        inner.tripped_at_ns = now;
        inner.state = State::Open;
        inner.failures = self.failure_threshold;
        self.is_open.store(true, Ordering::SeqCst);
        tracing::error!("Circuit breaker manually tripped");
    }

    pub fn can_execute(&self) -> bool {
        let mut inner = self.inner.lock().unwrap();

        match inner.state {
            State::Closed => true,
            State::Open => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;
                let elapsed_ms = now.saturating_sub(inner.tripped_at_ns) / 1_000_000;

                if elapsed_ms > self.cooldown_ms {
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
        let mut inner = self.inner.lock().unwrap();
        inner.failures = 0;
        inner.state = State::Closed;
        self.is_open.store(false, Ordering::SeqCst);
    }

    pub fn failure_count(&self) -> u64 {
        self.inner.lock().unwrap().failures
    }

    pub fn state_name(&self) -> &'static str {
        let inner = self.inner.lock().unwrap();
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
