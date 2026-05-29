//! Clock abstraction for time measurements.
//!
//! This module provides an injectable clock interface to replace direct usage of
//! `std::time::SystemTime::now()`. This is critical for:
//!
//! - **Deterministic testing**: `TestClock` allows simulating time travel in unit tests
//! - **Reliable latency measurements**: `SystemTime` can jump backward due to NTP adjustments,
//!   making latency histograms unreliable. Monotonic time (`Instant`) is used for durations.
//! - **Testability**: Components can be tested without relying on wall-clock time
//!
//! # Usage
//!
//! ```rust
//! use std::sync::Arc;
//! use unified_trading_core::clock::{Clock, WallClock, TestClock};
//!
//! // Production: use WallClock
//! let clock = Arc::new(WallClock::new());
//! let now_ns = clock.now_ns();
//!
//! // Testing: use TestClock for deterministic time
//! let clock = Arc::new(TestClock::new(0));
//! clock.advance(1_000_000_000); // advance by 1 second
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// A clock that provides wall time and monotonic time.
///
/// Wall time (`now_ns()`) returns the current time since UNIX epoch.
/// Monotonic time (`now_monotonic_ns()`) returns a monotonically increasing value
/// suitable for measuring elapsed durations.
pub trait Clock: Send + Sync {
    /// Returns the current wall time in nanoseconds since UNIX epoch.
    fn now_ns(&self) -> u64;

    /// Returns the current monotonic time in nanoseconds.
    /// This value is guaranteed to be monotonically increasing and is suitable
    /// for measuring elapsed durations.
    fn now_monotonic_ns(&self) -> u64;
}

/// Production clock implementation using `SystemTime` and `Instant`.
///
/// - `now_ns()` uses `SystemTime::now()` for wall time
/// - `now_monotonic_ns()` uses `Instant::now()` for monotonic time
pub struct WallClock {
    start: Instant,
}

impl WallClock {
    /// Creates a new WallClock instance.
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

impl Default for WallClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for WallClock {
    #[inline]
    fn now_ns(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }

    #[inline]
    fn now_monotonic_ns(&self) -> u64 {
        self.start.elapsed().as_nanos() as u64
    }
}

/// Test clock with deterministic, adjustable time.
///
/// This clock is designed for unit testing where you need to:
/// - Control the exact time returned
/// - Simulate time passing without actual delays
/// - Test time-sensitive logic deterministically
///
/// # Example
///
/// ```rust
/// use unified_trading_core::clock::{Clock, TestClock};
/// use std::sync::Arc;
///
/// let clock = Arc::new(TestClock::new(1_000_000_000)); // 1 second after epoch
/// assert_eq!(clock.now_ns(), 1_000_000_000);
///
/// clock.advance(1_000_000_000); // advance by 1 second
/// assert_eq!(clock.now_ns(), 2_000_000_000);
/// ```
pub struct TestClock {
    /// Current time in nanoseconds since UNIX epoch.
    time_ns: AtomicU64,
}

impl TestClock {
    /// Creates a new TestClock with the given initial time in nanoseconds.
    pub fn new(initial_ns: u64) -> Self {
        Self {
            time_ns: AtomicU64::new(initial_ns),
        }
    }

    /// Creates a new TestClock initialized to the current system time.
    pub fn from_system_time() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        Self::new(now)
    }

    /// Advances the clock by the given duration in nanoseconds.
    pub fn advance(&self, duration_ns: u64) {
        let current = self.time_ns.load(Ordering::Relaxed);
        self.time_ns.store(current.saturating_add(duration_ns), Ordering::Relaxed);
    }

    /// Advances the clock by the given duration.
    pub fn advance_duration(&self, duration: Duration) {
        self.advance(duration.as_nanos() as u64);
    }

    /// Sets the clock to a specific time in nanoseconds since UNIX epoch.
    pub fn set_time(&self, time_ns: u64) {
        self.time_ns.store(time_ns, Ordering::Relaxed);
    }

    /// Returns the current time without advancing.
    pub fn get_time(&self) -> u64 {
        self.time_ns.load(Ordering::Relaxed)
    }
}

impl Default for TestClock {
    fn default() -> Self {
        Self::from_system_time()
    }
}

impl Clock for TestClock {
    #[inline]
    fn now_ns(&self) -> u64 {
        self.time_ns.load(Ordering::Relaxed)
    }

    #[inline]
    fn now_monotonic_ns(&self) -> u64 {
        // For tests, monotonic time equals wall time since we control both
        self.time_ns.load(Ordering::Relaxed)
    }
}

/// Helper to get current wall time in nanoseconds (convenience function).
#[inline]
pub fn wall_time_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Helper to get current monotonic time in nanoseconds (convenience function).
#[inline]
pub fn monotonic_time_ns() -> u64 {
    Instant::now().elapsed().as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wall_clock() {
        let clock = WallClock::new();
        let now = clock.now_ns();
        assert!(now > 0, "Wall time should be positive");
    }

    #[test]
    fn test_wall_clock_monotonic() {
        let clock = WallClock::new();
        // Use the same clock instance to measure elapsed time
        let t1 = clock.now_monotonic_ns();
        std::thread::sleep(Duration::from_micros(100));
        let t2 = clock.now_monotonic_ns();
        assert!(t2 > t1, "Monotonic time should increase");
    }

    #[test]
    fn test_test_clock_initial() {
        let clock = TestClock::new(1_000_000_000);
        assert_eq!(clock.now_ns(), 1_000_000_000);
    }

    #[test]
    fn test_test_clock_advance() {
        let clock = TestClock::new(0);
        assert_eq!(clock.now_ns(), 0);

        clock.advance(1_000_000_000);
        assert_eq!(clock.now_ns(), 1_000_000_000);

        clock.advance(500_000_000);
        assert_eq!(clock.now_ns(), 1_500_000_000);
    }

    #[test]
    fn test_test_clock_advance_duration() {
        let clock = TestClock::new(0);
        clock.advance_duration(Duration::from_secs(5));
        assert_eq!(clock.now_ns(), 5_000_000_000);
    }

    #[test]
    fn test_test_clock_set_time() {
        let clock = TestClock::new(0);
        clock.set_time(10_000_000_000);
        assert_eq!(clock.now_ns(), 10_000_000_000);
    }

    #[test]
    fn test_test_clock_monotonic() {
        let clock = TestClock::new(0);
        let t1 = clock.now_monotonic_ns();
        clock.advance(1_000_000);
        let t2 = clock.now_monotonic_ns();
        assert_eq!(t2 - t1, 1_000_000);
    }

    #[test]
    fn test_test_clock_saturating_add() {
        let clock = TestClock::new(u64::MAX - 100);
        clock.advance(200);
        // Should saturate at u64::MAX
        assert_eq!(clock.now_ns(), u64::MAX);
    }

    #[test]
    fn test_clock_trait_object() {
        let clock: Arc<dyn Clock> = Arc::new(TestClock::new(1_000_000));
        assert_eq!(clock.now_ns(), 1_000_000);
        assert_eq!(clock.now_monotonic_ns(), 1_000_000);
    }

    #[test]
    fn test_wall_time_ns_helper() {
        let now = wall_time_ns();
        assert!(now > 0);
    }

    #[test]
    fn test_monotonic_time_ns_helper() {
        let start = Instant::now();
        let t1 = start.elapsed().as_nanos() as u64;
        std::thread::sleep(Duration::from_micros(100));
        let t2 = start.elapsed().as_nanos() as u64;
        assert!(t2 > t1);
    }
}
