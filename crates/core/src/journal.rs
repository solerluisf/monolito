use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver, Sender};

use crate::metrics::GlobalMetrics;
use crate::threading::{spawn_pinned, ThreadPriority};

#[derive(Debug, Clone)]
pub enum JournalEntry {
    Tick { symbol: String, timestamp_ns: u64, data: String },
    Intent { symbol: String, timestamp_ns: u64, data: String },
    Fill { symbol: String, timestamp_ns: u64, data: String },
    Order { symbol: String, timestamp_ns: u64, data: String },
    Snapshot { timestamp_ns: u64, data: String },
    Event { event_type: String, timestamp_ns: u64, data: String },
}

#[derive(Debug)]
pub enum JournalCommand {
    Write(JournalEntry),
    Flush { ack: Sender<()> },
}

pub struct JournalWriter {
    pub tx: Sender<JournalCommand>,
    handle: Option<thread::JoinHandle<()>>,
    write_count: Arc<AtomicU64>,
    metrics: Arc<GlobalMetrics>,
}

impl JournalWriter {
    pub fn new(
        journal_dir: &str,
        flush_interval_ms: u64,
        metrics: Arc<GlobalMetrics>,
        core_id: usize,
    ) -> Self {
        let (tx, rx) = bounded::<JournalCommand>(10_000);
        let write_count = Arc::new(AtomicU64::new(0));

        let path = PathBuf::from(journal_dir);
        std::fs::create_dir_all(&path).ok();
        let file_path = path.join(format!(
            "journal_{}.log",
            chrono::Utc::now().format("%Y%m%d_%H%M%S")
        ));

        let wc = Arc::clone(&write_count);
        let metrics_clone = Arc::clone(&metrics);
        let handle = spawn_pinned(
            "journal",
            core_id,
            ThreadPriority::Normal,
            move || {
                Self::run_loop(rx, &file_path, flush_interval_ms, &metrics_clone, &wc);
            },
        );

        Self {
            tx,
            handle: Some(handle),
            write_count,
            metrics,
        }
    }

    fn run_loop(
        rx: Receiver<JournalCommand>,
        file_path: &PathBuf,
        flush_interval_ms: u64,
        metrics: &GlobalMetrics,
        write_count: &AtomicU64,
    ) {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)
            .expect("Failed to open journal file");
        let mut writer = BufWriter::new(file);

        let mut last_flush = std::time::Instant::now();
        let flush_dur = Duration::from_millis(flush_interval_ms);

        loop {
            match rx.recv_timeout(Duration::from_millis(10)) {
                Ok(JournalCommand::Write(entry)) => {
                    metrics.journal_channel_depth.fetch_sub(1, Ordering::Relaxed);
                    let line = format_entry(&entry);
                    if let Err(e) = writeln!(writer, "{}", line) {
                        eprintln!("Journal write error: {}", e);
                        metrics.errors.fetch_add(1, Ordering::Relaxed);
                    } else {
                        write_count.fetch_add(1, Ordering::Relaxed);
                        metrics.journal_writes.fetch_add(1, Ordering::Relaxed);
                    }

                    if last_flush.elapsed() >= flush_dur {
                        let _ = writer.flush();
                        last_flush = std::time::Instant::now();
                    }
                }
                Ok(JournalCommand::Flush { ack }) => {
                    metrics.journal_channel_depth.fetch_sub(1, Ordering::Relaxed);
                    let _ = writer.flush();
                    let _ = ack.send(());
                    last_flush = std::time::Instant::now();
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if last_flush.elapsed() >= flush_dur {
                        let _ = writer.flush();
                        last_flush = std::time::Instant::now();
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    let _ = writer.flush();
                    break;
                }
            }
        }
    }

    pub fn write(&self, entry: JournalEntry) -> Result<(), &'static str> {
        self.metrics.journal_channel_depth.fetch_add(1, Ordering::Relaxed);
        self.tx
            .send(JournalCommand::Write(entry))
            .map_err(|_| "Journal channel closed")
    }

    /// Synchronously flush the journal to disk.
    /// Blocks until the flush is complete.
    pub fn flush_sync(&self) -> Result<(), &'static str> {
        let (ack_tx, ack_rx) = bounded::<()>(1);
        self.metrics.journal_channel_depth.fetch_add(1, Ordering::Relaxed);
        let flush_start = std::time::Instant::now();
        self.tx
            .send(JournalCommand::Flush { ack: ack_tx })
            .map_err(|_| "Journal channel closed")?;
        let result = ack_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| "Flush timeout");
        let elapsed_ns = flush_start.elapsed().as_nanos() as u64;
        self.metrics.journal_flush_latency.record(elapsed_ns);
        result
    }

    pub fn write_count(&self) -> u64 {
        self.write_count.load(Ordering::Relaxed)
    }

    pub fn shutdown(self) {
        drop(self.tx);
        if let Some(handle) = self.handle {
            let _ = handle.join();
        }
    }
}

fn format_entry(entry: &JournalEntry) -> String {
    match entry {
        JournalEntry::Tick { symbol, timestamp_ns, data } => {
            format!("TICK|{}|{}|{}", symbol, timestamp_ns, data)
        }
        JournalEntry::Intent { symbol, timestamp_ns, data } => {
            format!("INTENT|{}|{}|{}", symbol, timestamp_ns, data)
        }
        JournalEntry::Fill { symbol, timestamp_ns, data } => {
            format!("FILL|{}|{}|{}", symbol, timestamp_ns, data)
        }
        JournalEntry::Order { symbol, timestamp_ns, data } => {
            format!("ORDER|{}|{}|{}", symbol, timestamp_ns, data)
        }
        JournalEntry::Snapshot { timestamp_ns, data } => {
            format!("SNAPSHOT|{}|{}", timestamp_ns, data)
        }
        JournalEntry::Event { event_type, timestamp_ns, data } => {
            format!("EVENT|{}|{}|{}", event_type, timestamp_ns, data)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_journal_write_and_shutdown() {
        let metrics = Arc::new(GlobalMetrics::new());
        let tmp_dir = std::env::temp_dir()
            .join(format!("journal_test_{}", std::process::id()));
        let writer = JournalWriter::new(
            tmp_dir.to_str().unwrap(),
            50,
            Arc::clone(&metrics),
            0, // core_id
        );

        let entry = JournalEntry::Tick {
            symbol: "AAPL".to_string(),
            timestamp_ns: 12345,
            data: "price=150.0".to_string(),
        };
        assert!(writer.write(entry).is_ok());

        std::thread::sleep(std::time::Duration::from_millis(20));
        let count = writer.write_count();
        assert!(count > 0);

        writer.shutdown();
        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn test_journal_entry_format() {
        let entry = JournalEntry::Tick {
            symbol: "MSFT".to_string(),
            timestamp_ns: 100,
            data: "test".to_string(),
        };
        let formatted = format_entry(&entry);
        assert!(formatted.starts_with("TICK|MSFT|100|"));
    }

    #[test]
    fn test_journal_multiple_entries() {
        let metrics = Arc::new(GlobalMetrics::new());
        let tmp_dir = std::env::temp_dir()
            .join(format!("journal_test2_{}", std::process::id()));
        let writer = JournalWriter::new(
            tmp_dir.to_str().unwrap(),
            10,
            Arc::clone(&metrics),
            0, // core_id
        );

        for i in 0..10 {
            let entry = JournalEntry::Intent {
                symbol: "AAPL".to_string(),
                timestamp_ns: i,
                data: format!("intent_{}", i),
            };
            writer.write(entry).unwrap();
        }

        std::thread::sleep(Duration::from_millis(50));
        let count = writer.write_count();
        assert_eq!(count, 10);

        writer.shutdown();
        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn test_journal_flush_sync() {
        let metrics = Arc::new(GlobalMetrics::new());
        let tmp_dir = std::env::temp_dir()
            .join(format!("journal_test3_{}", std::process::id()));
        let writer = JournalWriter::new(
            tmp_dir.to_str().unwrap(),
            5000, // Long flush interval to ensure sync flush works
            Arc::clone(&metrics),
            0, // core_id
        );

        let entry = JournalEntry::Tick {
            symbol: "AAPL".to_string(),
            timestamp_ns: 12345,
            data: "price=150.0".to_string(),
        };
        assert!(writer.write(entry).is_ok());
        
        // Sync flush should complete without error
        assert!(writer.flush_sync().is_ok());

        let count = writer.write_count();
        assert_eq!(count, 1);

        writer.shutdown();
        let _ = std::fs::remove_dir_all(&tmp_dir);
    }
}
