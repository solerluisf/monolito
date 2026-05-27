use std::sync::atomic::Ordering;
use std::time::Duration;

use crossbeam_channel::{Receiver, SendTimeoutError, Sender, TrySendError};

use crate::config::BackpressurePolicy;
use crate::metrics::GlobalMetrics;

/// Result type for policy-aware send operations.
#[derive(Debug)]
pub enum PolicySendError<T> {
    Dropped(T),
    Timeout(T),
    Disconnected(T),
}

/// Simple helper that sends a message on a bounded channel.
/// If the channel is full, increments the dropped counter and returns the message.
pub fn try_send_or_drop<T>(
    tx: &Sender<T>,
    msg: T,
    metrics: &GlobalMetrics,
) -> Result<(), T> {
    match tx.try_send(msg) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(m)) => {
            metrics.dropped_intents.fetch_add(1, Ordering::Relaxed);
            Err(m)
        }
        Err(TrySendError::Disconnected(m)) => Err(m),
    }
}

/// Sends a message on a bounded channel, dropping the oldest message if the channel is full.
/// This requires access to both the sender and receiver ends.
/// Returns Ok(()) if the message was sent, or Err(msg) if the channel is disconnected.
pub fn try_send_drop_oldest<T>(
    tx: &Sender<T>,
    rx: &Receiver<T>,
    msg: T,
    metrics: &GlobalMetrics,
) -> Result<(), T> {
    match tx.try_send(msg) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(m)) => {
            // Try to make room by receiving and dropping oldest message
            match rx.try_recv() {
                Ok(_dropped) => {
                    // Successfully dropped oldest, now try to send again
                    metrics.dropped_intents.fetch_add(1, Ordering::Relaxed);
                    match tx.try_send(m) {
                        Ok(()) => Ok(()),
                        Err(TrySendError::Full(m)) | Err(TrySendError::Disconnected(m)) => Err(m),
                    }
                }
                Err(_) => {
                    // Channel empty or disconnected, just return the message
                    Err(m)
                }
            }
        }
        Err(TrySendError::Disconnected(m)) => Err(m),
    }
}

/// Policy-aware send helper.
///
/// Notes:
/// - `DropOldest` requires `rx_for_drop_oldest` and will behave like a circular buffer.
/// - `BlockWithTimeoutMs` uses `send_timeout`.
/// - `PrioritizeCritical` defaults to non-critical behavior (`DropNewest`) here; use
///   `send_with_policy_critical` for critical-path sending.
pub fn send_with_policy<T>(
    tx: &Sender<T>,
    rx_for_drop_oldest: Option<&Receiver<T>>,
    msg: T,
    policy: &BackpressurePolicy,
    metrics: &GlobalMetrics,
) -> Result<(), PolicySendError<T>> {
    match policy {
        BackpressurePolicy::DropNewest => match tx.try_send(msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(m)) => {
                metrics.dropped_intents.fetch_add(1, Ordering::Relaxed);
                Err(PolicySendError::Dropped(m))
            }
            Err(TrySendError::Disconnected(m)) => Err(PolicySendError::Disconnected(m)),
        },
        BackpressurePolicy::DropOldest => {
            let rx = match rx_for_drop_oldest {
                Some(r) => r,
                None => {
                    return match tx.try_send(msg) {
                        Ok(()) => Ok(()),
                        Err(TrySendError::Full(m)) => {
                            metrics.dropped_intents.fetch_add(1, Ordering::Relaxed);
                            Err(PolicySendError::Dropped(m))
                        }
                        Err(TrySendError::Disconnected(m)) => Err(PolicySendError::Disconnected(m)),
                    };
                }
            };

            match tx.try_send(msg) {
                Ok(()) => Ok(()),
                Err(TrySendError::Full(m)) => {
                    match rx.try_recv() {
                        Ok(_oldest) => {
                            metrics.dropped_intents.fetch_add(1, Ordering::Relaxed);
                            match tx.try_send(m) {
                                Ok(()) => Ok(()),
                                Err(TrySendError::Full(m2)) => Err(PolicySendError::Dropped(m2)),
                                Err(TrySendError::Disconnected(m2)) => Err(PolicySendError::Disconnected(m2)),
                            }
                        }
                        Err(_) => Err(PolicySendError::Dropped(m)),
                    }
                }
                Err(TrySendError::Disconnected(m)) => Err(PolicySendError::Disconnected(m)),
            }
        }
        BackpressurePolicy::BlockWithTimeoutMs(timeout_ms) => {
            let timeout = Duration::from_millis(*timeout_ms);
            match tx.send_timeout(msg, timeout) {
                Ok(()) => Ok(()),
                Err(SendTimeoutError::Timeout(m)) => Err(PolicySendError::Timeout(m)),
                Err(SendTimeoutError::Disconnected(m)) => Err(PolicySendError::Disconnected(m)),
            }
        }
        BackpressurePolicy::PrioritizeCritical => {
            // Non-critical default: DropNewest behavior.
            match tx.try_send(msg) {
                Ok(()) => Ok(()),
                Err(TrySendError::Full(m)) => {
                    metrics.dropped_intents.fetch_add(1, Ordering::Relaxed);
                    Err(PolicySendError::Dropped(m))
                }
                Err(TrySendError::Disconnected(m)) => Err(PolicySendError::Disconnected(m)),
            }
        }
    }
}

/// Critical-path variant for `PrioritizeCritical`.
/// For now this attempts to preserve critical sends by dropping oldest when full.
pub fn send_with_policy_critical<T>(
    tx: &Sender<T>,
    rx_for_drop_oldest: Option<&Receiver<T>>,
    msg: T,
    policy: &BackpressurePolicy,
    metrics: &GlobalMetrics,
) -> Result<(), PolicySendError<T>> {
    match policy {
        BackpressurePolicy::PrioritizeCritical => {
            // Treat as drop-oldest for critical messages.
            send_with_policy(
                tx,
                rx_for_drop_oldest,
                msg,
                &BackpressurePolicy::DropOldest,
                metrics,
            )
        }
        _ => send_with_policy(tx, rx_for_drop_oldest, msg, policy, metrics),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BackpressurePolicy;
    use crossbeam_channel::bounded;

    #[test]
    fn test_try_send_or_drop_success() {
        let (tx, rx) = bounded::<i32>(10);
        let metrics = GlobalMetrics::new();
        let result = try_send_or_drop(&tx, 42, &metrics);
        assert!(result.is_ok());
        assert_eq!(rx.try_recv().unwrap(), 42);
    }

    #[test]
    fn test_try_send_or_drop_full() {
        let (tx, _rx) = bounded::<i32>(1);
        let metrics = GlobalMetrics::new();
        // Fill the channel
        tx.try_send(1).unwrap();

        // Try to send another - should drop
        let result = try_send_or_drop(&tx, 2, &metrics);
        assert!(result.is_err());
    }

    #[test]
    fn test_try_send_drop_oldest() {
        let (tx, rx) = bounded::<i32>(2);
        let metrics = GlobalMetrics::new();
        // Fill the channel
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();

        // Try to send another - should drop oldest (1) and accept new
        let result = try_send_drop_oldest(&tx, &rx, 3, &metrics);
        assert!(result.is_ok());

        // Verify oldest was dropped and new message is there
        assert_eq!(rx.try_recv().unwrap(), 2);
        assert_eq!(rx.try_recv().unwrap(), 3);
    }

    #[test]
    fn test_drop_oldest_retains_newest_100_after_200_sends() {
        let (tx, rx) = bounded::<u64>(100);
        let metrics = GlobalMetrics::new();
        for i in 0..200u64 {
            let res = send_with_policy(
                &tx,
                Some(&rx),
                i,
                &BackpressurePolicy::DropOldest,
                &metrics,
            );
            assert!(res.is_ok());
        }

        let mut out = Vec::new();
        while let Ok(v) = rx.try_recv() {
            out.push(v);
        }

        assert_eq!(out.len(), 100);
        assert_eq!(out[0], 100);
        assert_eq!(out[99], 199);
    }

    #[test]
    fn test_block_with_timeout_times_out_when_full() {
        let (tx, _rx) = bounded::<u64>(1);
        let metrics = GlobalMetrics::new();
        // Fill channel
        tx.send(1).unwrap();

        // This should timeout (not drop) because nobody is receiving.
        let res = send_with_policy(
            &tx,
            None,
            2,
            &BackpressurePolicy::BlockWithTimeoutMs(5),
            &metrics,
        );

        match res {
            Err(PolicySendError::Timeout(v)) => assert_eq!(v, 2),
            other => panic!("expected timeout, got {:?}", other),
        }

    }
}
