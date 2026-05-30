use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, UNIX_EPOCH};

use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};

use crate::clock::{Clock, WallClock};
use crate::metrics::GlobalMetrics;
use crate::threading::{spawn_pinned, ThreadPriority};

/// A `Send + Sync` handle for non-blocking journal writes from hot paths.
/// Clones the channel sender and metric references — lightweight.
pub struct JournalHandle {
    tx: Sender<JournalCommand>,
    metrics: Arc<GlobalMetrics>,
}

impl JournalHandle {
    pub fn try_write(&self, entry: JournalEntry) -> Result<(), &'static str> {
        self.metrics.journal_channel_depth.fetch_add(1, Ordering::Relaxed);
        match self.tx.try_send(JournalCommand::Write(entry)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                self.metrics.journal_channel_depth.fetch_sub(1, Ordering::Relaxed);
                self.metrics.errors.fetch_add(1, Ordering::Relaxed);
                Err("Journal channel full")
            }
            Err(TrySendError::Disconnected(_)) => {
                self.metrics.journal_channel_depth.fetch_sub(1, Ordering::Relaxed);
                Err("Journal channel closed")
            }
        }
    }

    pub fn tx(&self) -> Sender<JournalCommand> {
        self.tx.clone()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum JournalError {
    Io(String),
    Parse(String),
}

impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JournalError::Io(msg) => write!(f, "IO error: {}", msg),
            JournalError::Parse(msg) => write!(f, "Parse error: {}", msg),
        }
    }
}

impl std::error::Error for JournalError {}

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
    Compaction,
}

pub struct JournalWriter {
    pub tx: Sender<JournalCommand>,
    handle: Option<thread::JoinHandle<()>>,
    write_count: Arc<AtomicU64>,
    metrics: Arc<GlobalMetrics>,
    journal_dir: PathBuf,
    retention_hours: u32,
    max_size_mb: u64,
    clock: Arc<dyn Clock>,
}

impl JournalWriter {
    pub fn new(
        journal_dir: &str,
        flush_interval_ms: u64,
        metrics: Arc<GlobalMetrics>,
        core_id: usize,
        retention_hours: u32,
        max_size_mb: u64,
    ) -> Self {
        Self::with_clock(
            journal_dir,
            flush_interval_ms,
            metrics,
            core_id,
            retention_hours,
            max_size_mb,
            Arc::new(WallClock::new()),
        )
    }

    pub fn with_clock(
        journal_dir: &str,
        flush_interval_ms: u64,
        metrics: Arc<GlobalMetrics>,
        core_id: usize,
        retention_hours: u32,
        max_size_mb: u64,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let (tx, rx) = bounded::<JournalCommand>(10_000);
        let write_count = Arc::new(AtomicU64::new(0));

        let path = PathBuf::from(journal_dir);
        fs::create_dir_all(&path).ok();
        let file_path = path.join(format!(
            "journal_{}.log",
            chrono::Utc::now().format("%Y%m%d_%H%M%S")
        ));

        let wc = Arc::clone(&write_count);
        let metrics_clone = Arc::clone(&metrics);
        let journal_dir_clone = path.clone();
        let retention_hours_clone = retention_hours;
        let max_size_mb_clone = max_size_mb;
        let clock_clone = Arc::clone(&clock);

        let handle = spawn_pinned(
            "journal",
            core_id,
            ThreadPriority::Normal,
            move || {
                Self::run_loop(
                    rx,
                    &file_path,
                    flush_interval_ms,
                    &metrics_clone,
                    &wc,
                    retention_hours_clone,
                    max_size_mb_clone,
                    &journal_dir_clone,
                    &clock_clone,
                );
            },
        );

        Self {
            tx,
            handle: Some(handle.expect("spawn_pinned failed")),
            write_count,
            metrics,
            journal_dir: path,
            retention_hours,
            max_size_mb,
            clock,
        }
    }

    pub fn replay<F>(&self, mut handler: F) -> Result<u64, JournalError>
    where
        F: FnMut(&JournalEntry),
    {
        let mut entries_read: u64 = 0;
        let mut files: Vec<std::path::PathBuf> = Vec::new();

        if let Ok(entries) = fs::read_dir(&self.journal_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(name) = path.file_name() {
                    let name_str = name.to_string_lossy();
                    if name_str.starts_with("journal_") && name_str.ends_with(".log") {
                        files.push(path);
                    }
                }
            }
        }

        files.sort();

        for file_path in files {
            let content =
                fs::read_to_string(&file_path).map_err(|e| JournalError::Io(format!("Failed to read {:?}: {}", file_path, e)))?;

            for line in content.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                if let Ok(entry) = parse_entry_line(line) {
                    handler(&entry);
                    entries_read += 1;
                }
            }
        }

        Ok(entries_read)
    }

    fn run_loop(
        rx: Receiver<JournalCommand>,
        file_path: &PathBuf,
        flush_interval_ms: u64,
        metrics: &GlobalMetrics,
        write_count: &AtomicU64,
        retention_hours: u32,
        max_size_mb: u64,
        journal_dir: &PathBuf,
        clock: &Arc<dyn Clock>,
    ) {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)
            .expect("Failed to open journal file");
        let mut writer = BufWriter::new(file);

        let mut last_flush = std::time::Instant::now();
        let flush_dur = Duration::from_millis(flush_interval_ms);
        let mut last_compaction_check = std::time::Instant::now();
        let compaction_check_interval = Duration::from_secs(3600);

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
                Ok(JournalCommand::Compaction) => {
                    let _ = writer.flush();
                    Self::run_compaction(journal_dir, retention_hours, max_size_mb, clock);
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if last_flush.elapsed() >= flush_dur {
                        let _ = writer.flush();
                        last_flush = std::time::Instant::now();
                    }
                    if last_compaction_check.elapsed() >= compaction_check_interval {
                        let _ = writer.flush();
                        Self::run_compaction(journal_dir, retention_hours, max_size_mb, clock);
                        last_compaction_check = std::time::Instant::now();
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    let _ = writer.flush();
                    break;
                }
            }
        }
    }

    fn run_compaction(journal_dir: &PathBuf, retention_hours: u32, max_size_mb: u64, clock: &Arc<dyn Clock>) {
        let now = clock.now_ns() / 1_000_000_000; // Convert ns to seconds
        let retention_secs = retention_hours as u64 * 3600;
        let cutoff = now.saturating_sub(retention_secs);
        let max_bytes = max_size_mb as u64 * 1024 * 1024;

        if let Ok(entries) = fs::read_dir(journal_dir) {
            let mut journal_files: Vec<(u64, std::path::PathBuf)> = Vec::new();
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(name) = path.file_name() {
                    let name_str = name.to_string_lossy();
                    if name_str.starts_with("journal_") && name_str.ends_with(".log") {
                        if let Ok(metadata) = path.metadata() {
                            let modified = metadata
                                .modified()
                                .ok()
                                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                                .map(|d| d.as_secs())
                                .unwrap_or(0);
                            journal_files.push((modified, path));
                        }
                    }
                }
            }

            journal_files.sort_by_key(|k| k.0);

            let mut total_size: u64 = journal_files.iter().map(|(_, p)| {
                p.metadata().map(|m| m.len()).unwrap_or(0)
            }).sum();

            for (modified, path) in journal_files {
                if modified < cutoff {
                    tracing::info!("Deleting expired journal file: {:?}", path);
                    if fs::remove_file(&path).is_ok() {
                        if let Ok(meta) = path.metadata() {
                            total_size = total_size.saturating_sub(meta.len());
                        }
                    }
                    continue;
                }

                if total_size > max_bytes {
                    tracing::info!("Deleting oldest journal file to reduce size: {:?}", path);
                    if fs::remove_file(&path).is_ok() {
                        if let Ok(meta) = path.metadata() {
                            total_size = total_size.saturating_sub(meta.len());
                        }
                    }
                }

                if total_size <= max_bytes {
                    break;
                }
            }
        }
    }

    pub fn trigger_compaction(&self) {
        let _ = self.tx.send(JournalCommand::Compaction);
    }

    /// Returns a `JournalHandle` suitable for sharing across threads (Send + Sync).
    /// The handle clones the channel sender and metric reference — cheap.
    pub fn handle(&self) -> JournalHandle {
        JournalHandle {
            tx: self.tx.clone(),
            metrics: Arc::clone(&self.metrics),
        }
    }

    pub fn write(&self, entry: JournalEntry) -> Result<(), &'static str> {
        self.metrics.journal_channel_depth.fetch_add(1, Ordering::Relaxed);
        self.tx.send(JournalCommand::Write(entry)).map_err(|_| "Journal channel closed")
    }

    /// Non-blocking write for hot paths.
    /// If the channel is full, entry is dropped to avoid stalling safety-critical execution.
    pub fn try_write(&self, entry: JournalEntry) -> Result<(), &'static str> {
        self.metrics.journal_channel_depth.fetch_add(1, Ordering::Relaxed);
        match self.tx.try_send(JournalCommand::Write(entry)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                self.metrics.journal_channel_depth.fetch_sub(1, Ordering::Relaxed);
                self.metrics.errors.fetch_add(1, Ordering::Relaxed);
                Err("Journal channel full")
            }
            Err(TrySendError::Disconnected(_)) => {
                self.metrics.journal_channel_depth.fetch_sub(1, Ordering::Relaxed);
                Err("Journal channel closed")
            }
        }
    }

    pub fn flush_sync(&self) -> Result<(), &'static str> {
        let (ack_tx, ack_rx) = bounded::<()>(1);
        self.metrics.journal_channel_depth.fetch_add(1, Ordering::Relaxed);
        let flush_start = std::time::Instant::now();
        self.tx.send(JournalCommand::Flush { ack: ack_tx }).map_err(|_| "Journal channel closed")?;
        let result = ack_rx.recv_timeout(Duration::from_secs(5)).map_err(|_| "Flush timeout");
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

fn parse_entry_line(line: &str) -> Result<JournalEntry, JournalError> {
    let parts: Vec<&str> = line.splitn(4, '|').collect();
    if parts.len() < 3 {
        return Err(JournalError::Parse("Invalid journal line format".to_string()));
    }

    let entry_type = parts[0];
    let timestamp_ns: u64 = parts[2].parse().map_err(|e| JournalError::Parse(format!("Bad timestamp: {}", e)))?;

    match entry_type {
        "TICK" => {
            if parts.len() != 4 {
                return Err(JournalError::Parse("Invalid TICK entry".to_string()));
            }
            Ok(JournalEntry::Tick {
                symbol: parts[1].to_string(),
                timestamp_ns,
                data: parts[3].to_string(),
            })
        }
        "INTENT" => {
            if parts.len() != 4 {
                return Err(JournalError::Parse("Invalid INTENT entry".to_string()));
            }
            Ok(JournalEntry::Intent {
                symbol: parts[1].to_string(),
                timestamp_ns,
                data: parts[3].to_string(),
            })
        }
        "FILL" => {
            if parts.len() != 4 {
                return Err(JournalError::Parse("Invalid FILL entry".to_string()));
            }
            Ok(JournalEntry::Fill {
                symbol: parts[1].to_string(),
                timestamp_ns,
                data: parts[3].to_string(),
            })
        }
        "ORDER" => {
            if parts.len() != 4 {
                return Err(JournalError::Parse("Invalid ORDER entry".to_string()));
            }
            Ok(JournalEntry::Order {
                symbol: parts[1].to_string(),
                timestamp_ns,
                data: parts[3].to_string(),
            })
        }
        "SNAPSHOT" => {
            if parts.len() == 3 {
                Ok(JournalEntry::Snapshot {
                    timestamp_ns,
                    data: parts[2].to_string(),
                })
            } else {
                Err(JournalError::Parse("Invalid SNAPSHOT entry".to_string()))
            }
        }
        "EVENT" => {
            if parts.len() != 4 {
                return Err(JournalError::Parse("Invalid EVENT entry".to_string()));
            }
            Ok(JournalEntry::Event {
                event_type: parts[1].to_string(),
                timestamp_ns,
                data: parts[3].to_string(),
            })
        }
        _ => Err(JournalError::Parse(format!("Unknown journal entry type: {}", entry_type))),
    }
}

fn format_entry(entry: &JournalEntry) -> String {
    match entry {
        JournalEntry::Tick {
            symbol, timestamp_ns, data
        } => format!("TICK|{}|{}|{}", symbol, timestamp_ns, data),
        JournalEntry::Intent {
            symbol, timestamp_ns, data
        } => format!("INTENT|{}|{}|{}", symbol, timestamp_ns, data),
        JournalEntry::Fill {
            symbol, timestamp_ns, data
        } => format!("FILL|{}|{}|{}", symbol, timestamp_ns, data),
        JournalEntry::Order {
            symbol, timestamp_ns, data
        } => format!("ORDER|{}|{}|{}", symbol, timestamp_ns, data),
        JournalEntry::Snapshot { timestamp_ns, data } => {
            format!("SNAPSHOT|{}|{}", timestamp_ns, data)
        }
        JournalEntry::Event {
            event_type, timestamp_ns, data
        } => format!("EVENT|{}|{}|{}", event_type, timestamp_ns, data),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_journal_write_and_shutdown() {
        let metrics = Arc::new(GlobalMetrics::new());
        let tmp_dir = std::env::temp_dir()
            .join(format!("journal_test_{}", std::process::id()));
        let writer = JournalWriter::new(
            tmp_dir.to_str().unwrap(),
            50,
            Arc::clone(&metrics),
            0,
            168,
            10_000,
        );

        let entry = JournalEntry::Tick {
            symbol: "AAPL".to_string(),
            timestamp_ns: 12345,
            data: "price=150.0".to_string(),
        };
        assert!(writer.write(entry).is_ok());
        assert!(writer.flush_sync().is_ok());
        assert_eq!(writer.write_count(), 1);

        writer.shutdown();
        let _ = fs::remove_dir_all(&tmp_dir);
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
            0,
            168,
            10_000,
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
        let _ = fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn test_journal_try_write_non_blocking() {
        let metrics = Arc::new(GlobalMetrics::new());
        let tmp_dir = std::env::temp_dir()
            .join(format!("journal_try_write_test_{}", std::process::id()));
        let writer = JournalWriter::new(
            tmp_dir.to_str().unwrap(),
            1000,
            Arc::clone(&metrics),
            0,
            168,
            10_000,
        );

        for i in 0..20_000 {
            let _ = writer.try_write(JournalEntry::Event {
                event_type: "TEST".to_string(),
                timestamp_ns: i,
                data: "payload".to_string(),
            });
        }

        writer.shutdown();
        let _ = fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn test_journal_flush_sync() {
        let metrics = Arc::new(GlobalMetrics::new());
        let tmp_dir = std::env::temp_dir()
            .join(format!("journal_test3_{}", std::process::id()));
        let writer = JournalWriter::new(
            tmp_dir.to_str().unwrap(),
            5000,
            Arc::clone(&metrics),
            0,
            168,
            10_000,
        );

        let entry = JournalEntry::Tick {
            symbol: "AAPL".to_string(),
            timestamp_ns: 12345,
            data: "price=150.0".to_string(),
        };
        assert!(writer.write(entry).is_ok());
        assert!(writer.flush_sync().is_ok());

        let count = writer.write_count();
        assert_eq!(count, 1);

        writer.shutdown();
        let _ = fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn test_journal_replay() {
        let metrics = Arc::new(GlobalMetrics::new());
        let tmp_dir = std::env::temp_dir()
            .join(format!("journal_replay_test_{}", std::process::id()));
        let writer = JournalWriter::new(tmp_dir.to_str().unwrap(), 10, Arc::clone(&metrics), 0, 168, 10_000);

        let entries = vec![
            JournalEntry::Order {
                symbol: "AAPL".to_string(),
                timestamp_ns: 100,
                data: "order_id=o1,side=Buy,qty=10,decision=req-1".to_string(),
            },
            JournalEntry::Fill {
                symbol: "AAPL".to_string(),
                timestamp_ns: 200,
                data: "price=150.0,qty=10".to_string(),
            },
            JournalEntry::Tick {
                symbol: "MSFT".to_string(),
                timestamp_ns: 300,
                data: "price=300.0".to_string(),
            },
        ];

        for entry in &entries {
            writer.write(entry.clone()).unwrap();
        }
        assert!(writer.flush_sync().is_ok());
        writer.shutdown();

        let writer2 = JournalWriter::new(tmp_dir.to_str().unwrap(), 10, Arc::clone(&metrics), 0, 168, 10_000);

        let mut replayed = Vec::new();
        let count = writer2
            .replay(|entry| {
                replayed.push(entry.clone());
            })
            .expect("replay should succeed");

        assert_eq!(count, 3);
        assert_eq!(replayed.len(), 3);
        assert!(matches!(replayed[0], JournalEntry::Order { .. }));
        assert!(matches!(replayed[1], JournalEntry::Fill { .. }));
        assert!(matches!(replayed[2], JournalEntry::Tick { .. }));

        writer2.shutdown();
        let _ = fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn test_compactionTrigger() {
        let metrics = Arc::new(GlobalMetrics::new());
        let tmp_dir = std::env::temp_dir()
            .join(format!("journal_compact_test_{}", std::process::id()));

        std::fs::create_dir(&tmp_dir).ok();

        let old_file = tmp_dir.join("journal_20200101_000000.log");
        std::fs::write(&old_file, "TICK|AAPL|100|old_data\n").ok();

        let writer = JournalWriter::new(
            tmp_dir.to_str().unwrap(),
            10,
            Arc::clone(&metrics),
            0,
            1,
            10_000,
        );

        writer.trigger_compaction();
        std::thread::sleep(Duration::from_millis(50));

        writer.shutdown();
        let _ = std::fs::remove_dir_all(&tmp_dir);
    }
}
