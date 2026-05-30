use std::collections::{HashMap, VecDeque};
use parking_lot::Mutex;
use std::time::Instant;

pub const DEFAULT_CAPACITY: usize = 100_000;
pub const DEFAULT_MAX_MEMORY_BYTES: usize = 8 * 1024 * 1024;

struct Entry {
    result: String,
    last_accessed: Instant,
}

impl Entry {
    fn estimated_bytes_for(key: &str, result: &str) -> usize {
        // Approximate in-memory footprint (string payloads + Entry metadata estimate).
        key.len() + result.len() + 64
    }
}

pub struct IdempotencyStore {
    processed: Mutex<HashMap<String, Entry>>,
    access_order: Mutex<VecDeque<String>>,
    capacity: usize,
    max_memory_bytes: usize,
    current_memory_bytes: Mutex<usize>,
    persist_path: Option<std::path::PathBuf>,
}

impl IdempotencyStore {
    pub const DEFAULT_CAPACITY: usize = DEFAULT_CAPACITY;

    pub fn new() -> Self {
        Self::with_limits(DEFAULT_CAPACITY, DEFAULT_MAX_MEMORY_BYTES)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_limits(capacity, DEFAULT_MAX_MEMORY_BYTES)
    }

    pub fn with_limits(capacity: usize, max_memory_bytes: usize) -> Self {
        Self {
            processed: Mutex::new(HashMap::with_capacity(capacity)),
            access_order: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            max_memory_bytes,
            current_memory_bytes: Mutex::new(0),
            persist_path: None,
        }
    }

    pub fn new_with_path(capacity: usize, path: &std::path::Path) -> Self {
        let mut store = Self {
            processed: Mutex::new(HashMap::with_capacity(capacity)),
            access_order: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            max_memory_bytes: DEFAULT_MAX_MEMORY_BYTES,
            current_memory_bytes: Mutex::new(0),
            persist_path: Some(path.to_path_buf()),
        };
        store.load_from_disk();
        store
    }

    fn load_from_disk(&mut self) {
        let Some(ref path) = self.persist_path else { return };
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                tracing::warn!("Failed to read idempotency store: {}", e);
                return;
            }
        };

        let mut processed = self.processed.lock();
        let mut access_order = self.access_order.lock();
        let mut current_memory_bytes = self.current_memory_bytes.lock();
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.splitn(2, ' ').collect();
            if parts.len() != 2 {
                continue;
            }
            let key = parts[0].to_string();
            let result = parts[1].to_string();
            *current_memory_bytes += Entry::estimated_bytes_for(&key, &result);
            let entry = Entry {
                result,
                last_accessed: std::time::Instant::now(),
            };
            processed.insert(key.clone(), entry);
            access_order.push_front(key);
        }

        Self::evict_until_under_cap(
            &mut processed,
            &mut access_order,
            &mut current_memory_bytes,
            self.capacity,
            self.max_memory_bytes,
            0,
            0,
        );
    }

    /// Evict LRU entries until both `capacity` and `max_memory_bytes` are satisfied.
    /// `reserve_slots` and `reserve_bytes` account for an upcoming insertion.
    fn evict_until_under_cap(
        processed: &mut HashMap<String, Entry>,
        access_order: &mut VecDeque<String>,
        current_memory_bytes: &mut usize,
        capacity: usize,
        max_memory_bytes: usize,
        reserve_slots: usize,
        reserve_bytes: usize,
    ) {
        while (processed.len() + reserve_slots > capacity || *current_memory_bytes + reserve_bytes > max_memory_bytes)
            && !access_order.is_empty()
        {
            if let Some(lru_key) = access_order.pop_back() {
                if let Some(evicted) = processed.remove(&lru_key) {
                    *current_memory_bytes = current_memory_bytes.saturating_sub(
                        Entry::estimated_bytes_for(&lru_key, &evicted.result),
                    );
                }
            }
        }
    }

    fn append_to_disk(&self, key: &str, result: &str) {
        let Some(ref path) = self.persist_path else { return };
        use std::io::Write;
        let mut file = match std::fs::OpenOptions::new().create(true).append(true).open(path) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!("Failed to open idempotency store for append: {}", e);
                return;
            }
        };
        if let Err(e) = writeln!(file, "{} {}", key, result) {
            tracing::warn!("Failed to append to idempotency store: {}", e);
        }
    }

    pub fn is_processed(&self, key: &str) -> bool {
        let mut processed = self.processed.lock();
        if !processed.contains_key(key) {
            return false;
        }

        let mut access_order = self.access_order.lock();
        let mut current_memory_bytes = self.current_memory_bytes.lock();

        if let Some(entry) = processed.get_mut(key) {
            entry.last_accessed = Instant::now();
        }

        if let Some(pos) = access_order.iter().position(|k| k == key) {
            access_order.remove(pos);
        }
        access_order.push_front(key.to_string());

        Self::evict_until_under_cap(
            &mut *processed,
            &mut *access_order,
            &mut *current_memory_bytes,
            self.capacity,
            self.max_memory_bytes,
            0,
            0,
        );

        true
    }

    pub fn mark_processed(&self, key: String, result: String) {
        let mut processed = self.processed.lock();
        let mut access_order = self.access_order.lock();
        let mut current_memory_bytes = self.current_memory_bytes.lock();

        if let Some(existing) = processed.remove(&key) {
            *current_memory_bytes = current_memory_bytes.saturating_sub(
                Entry::estimated_bytes_for(&key, &existing.result),
            );
            access_order.retain(|k| k != &key);
        }

        let new_entry_bytes = Entry::estimated_bytes_for(&key, &result);

        Self::evict_until_under_cap(
            &mut *processed,
            &mut *access_order,
            &mut *current_memory_bytes,
            self.capacity,
            self.max_memory_bytes,
            1,
            new_entry_bytes,
        );

        let entry = Entry {
            result: result.clone(),
            last_accessed: Instant::now(),
        };

        if new_entry_bytes > self.max_memory_bytes {
            return;
        }

        processed.insert(key.clone(), entry);
        access_order.push_front(key.clone());
        *current_memory_bytes += new_entry_bytes;

        drop(processed);
        drop(access_order);
        drop(current_memory_bytes);
        self.append_to_disk(&key, &result);
    }

    pub fn get_result(&self, key: &str) -> Option<String> {
        let mut processed = self.processed.lock();
        if !processed.contains_key(key) {
            return None;
        }

        let mut access_order = self.access_order.lock();
        let mut current_memory_bytes = self.current_memory_bytes.lock();

        let result = processed.get_mut(key).map(|e| {
            e.last_accessed = Instant::now();
            e.result.clone()
        });

        if let Some(pos) = access_order.iter().position(|k| k == key) {
            access_order.remove(pos);
        }
        access_order.push_front(key.to_string());

        Self::evict_until_under_cap(
            &mut *processed,
            &mut *access_order,
            &mut *current_memory_bytes,
            self.capacity,
            self.max_memory_bytes,
            0,
            0,
        );

        result
    }

    pub fn len(&self) -> usize {
        self.processed.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.processed.lock().is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn memory_usage_bytes(&self) -> usize {
        *self.current_memory_bytes.lock()
    }

    pub fn memory_limit_bytes(&self) -> usize {
        self.max_memory_bytes
    }

    pub fn clear(&self) {
        let mut processed = self.processed.lock();
        let mut access_order = self.access_order.lock();
        let mut current_memory_bytes = self.current_memory_bytes.lock();
        processed.clear();
        access_order.clear();
        *current_memory_bytes = 0;
        drop(processed);
        drop(access_order);
        drop(current_memory_bytes);
        if let Some(ref path) = self.persist_path {
            let _ = std::fs::remove_file(path);
        }
    }

}

impl Default for IdempotencyStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_store_is_empty() {
        let store = IdempotencyStore::new();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
        assert_eq!(store.capacity(), DEFAULT_CAPACITY);
    }

    #[test]
    fn test_mark_and_check_processed() {
        let store = IdempotencyStore::new();
        assert!(!store.is_processed("key1"));
        store.mark_processed("key1".to_string(), "result1".to_string());
        assert!(store.is_processed("key1"));
        assert!(!store.is_processed("key2"));
    }

    #[test]
    fn test_get_result() {
        let store = IdempotencyStore::new();
        store.mark_processed("key1".to_string(), "result1".to_string());
        assert_eq!(store.get_result("key1"), Some("result1".to_string()));
        assert_eq!(store.get_result("key2"), None);
    }

    #[test]
    fn test_eviction_when_at_capacity() {
        let store = IdempotencyStore::with_capacity(3);
        store.mark_processed("key1".to_string(), "result1".to_string());
        store.mark_processed("key2".to_string(), "result2".to_string());
        store.mark_processed("key3".to_string(), "result3".to_string());
        assert_eq!(store.len(), 3);

        store.mark_processed("key4".to_string(), "result4".to_string());
        assert_eq!(store.len(), 3);
        assert!(!store.is_processed("key1"));
        assert!(store.is_processed("key2"));
        assert!(store.is_processed("key3"));
        assert!(store.is_processed("key4"));
    }

    #[test]
    fn test_lru_order_updated_on_access() {
        let store = IdempotencyStore::with_capacity(3);
        store.mark_processed("key1".to_string(), "result1".to_string());
        store.mark_processed("key2".to_string(), "result2".to_string());
        store.mark_processed("key3".to_string(), "result3".to_string());

        assert!(store.is_processed("key1"));
        store.mark_processed("key4".to_string(), "result4".to_string());

        assert!(store.is_processed("key1"));
        assert!(!store.is_processed("key2"));
    }

    #[test]
    fn test_clear() {
        let store = IdempotencyStore::new();
        store.mark_processed("key1".to_string(), "result1".to_string());
        store.mark_processed("key2".to_string(), "result2".to_string());
        assert_eq!(store.len(), 2);
        store.clear();
        assert!(store.is_empty());
    }

    #[test]
    fn test_memory_cap_enforced() {
        let store = IdempotencyStore::with_limits(10_000, 256);

        for i in 0..200 {
            store.mark_processed(format!("key{}", i), "x".repeat(32));
        }

        assert!(store.memory_usage_bytes() <= store.memory_limit_bytes());
    }

    #[test]
    fn test_disk_persistence() {
        let tmp_path = std::env::temp_dir().join(format!("idempotency_test_{}", std::process::id()));
        {
            let store = IdempotencyStore::new_with_path(100, &tmp_path);
            store.mark_processed("key1".to_string(), "result1".to_string());
            store.mark_processed("key2".to_string(), "result2".to_string());
            assert_eq!(store.len(), 2);
            // store dropped here; file should persist
        }

        {
            let store2 = IdempotencyStore::new_with_path(100, &tmp_path);
            assert!(store2.is_processed("key1"));
            assert!(store2.is_processed("key2"));
            assert_eq!(store2.get_result("key1"), Some("result1".to_string()));
            assert_eq!(store2.len(), 2);
        }

        let _ = std::fs::remove_file(&tmp_path);
    }
}
