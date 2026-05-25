use std::sync::atomic::{AtomicU64, Ordering};
use crossbeam_channel::{Sender, Receiver, TrySendError};
use crate::metrics::GlobalMetrics;

/// Simple helper that sends a message on a bounded channel.
/// If the channel is full, increments the dropped counter and returns the message.
pub fn try_send_or_drop<T>(
    tx: &Sender<T>,
    msg: T,
    metrics: &GlobalMetrics,
    dropped_counter: &AtomicU64,
) -> Result<(), T> {
    match tx.try_send(msg) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(m)) => {
            dropped_counter.fetch_add(1, Ordering::Relaxed);
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
    dropped_counter: &AtomicU64,
) -> Result<(), T> {
    match tx.try_send(msg) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(m)) => {
            // Try to make room by receiving and dropping oldest message
            match rx.try_recv() {
                Ok(_dropped) => {
                    // Successfully dropped oldest, now try to send again
                    dropped_counter.fetch_add(1, Ordering::Relaxed);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::bounded;

    #[test]
    fn test_try_send_or_drop_success() {
        let (tx, rx) = bounded::<i32>(10);
        let metrics = GlobalMetrics::new();
        let counter = AtomicU64::new(0);

        let result = try_send_or_drop(&tx, 42, &metrics, &counter);
        assert!(result.is_ok());
        assert_eq!(rx.try_recv().unwrap(), 42);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_try_send_or_drop_full() {
        let (tx, _rx) = bounded::<i32>(1);
        let metrics = GlobalMetrics::new();
        let counter = AtomicU64::new(0);

        // Fill the channel
        tx.try_send(1).unwrap();

        // Try to send another - should drop
        let result = try_send_or_drop(&tx, 2, &metrics, &counter);
        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_try_send_drop_oldest() {
        let (tx, rx) = bounded::<i32>(2);
        let metrics = GlobalMetrics::new();
        let counter = AtomicU64::new(0);

        // Fill the channel
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();

        // Try to send another - should drop oldest (1) and accept new
        let result = try_send_drop_oldest(&tx, &rx, 3, &metrics, &counter);
        assert!(result.is_ok());
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        
        // Verify oldest was dropped and new message is there
        assert_eq!(rx.try_recv().unwrap(), 2);
        assert_eq!(rx.try_recv().unwrap(), 3);
    }
}
