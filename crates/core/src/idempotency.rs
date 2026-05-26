use std::collections::{HashMap, VecDeque};
use parking_lot::Mutex;
use std::time::Instant;

pub const DEFAULT_CAPACITY: usize = 100_000;

struct Entry {
    result: String,
    last_accessed: Instant,
}

pub struct IdempotencyStore {
    processed: Mutex<HashMap<String, Entry>>,
    access_order: Mutex<VecDeque<String>>,
    capacity: usize,
    persist_path: Option<std::path::PathBuf>,
}

impl IdempotencyStore {
    pub const DEFAULT_CAPACITY: usize = DEFAULT_CAPACITY;

    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            processed: Mutex::new(HashMap::with_capacity(capacity)),
            access_order: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            persist_path: None,
        }
    }

    pub fn new_with_path(capacity: usize, path: &std::path::Path) -> Self {
        let mut store = Self {
            processed: Mutex::new(HashMap::with_capacity(capacity)),
            access_order: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
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
            let entry = Entry {
                result,
                last_accessed: std::time::Instant::now(),
            };
            processed.insert(key.clone(), entry);
            access_order.push_front(key);
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

        if let Some(entry) = processed.get_mut(key) {
            entry.last_accessed = Instant::now();
            drop(processed);
            self.update_access_order(key);
            true
        } else {
            false
        }
    }

    pub fn mark_processed(&self, key: String, result: String) {
        let mut processed = self.processed.lock();
        let mut access_order = self.access_order.lock();

        if !processed.contains_key(&key) && processed.len() >= self.capacity {
            if let Some(lru_key) = access_order.pop_back() {
                processed.remove(&lru_key);
            }
        }

        if processed.contains_key(&key) {
            access_order.retain(|k| k != &key);
        }

        let entry = Entry {
            result: result.clone(),
            last_accessed: Instant::now(),
        };

        processed.insert(key.clone(), entry);
        access_order.push_front(key.clone());
        drop(processed);
        drop(access_order);
        self.append_to_disk(&key, &result);
    }

    pub fn get_result(&self, key: &str) -> Option<String> {
        let mut processed = self.processed.lock();

        if let Some(entry) = processed.get_mut(key) {
            entry.last_accessed = Instant::now();
            let result = entry.result.clone();
            drop(processed);
            self.update_access_order(key);
            Some(result)
        } else {
            None
        }
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

    pub fn clear(&self) {
        let mut processed = self.processed.lock();
        let mut access_order = self.access_order.lock();
        processed.clear();
        access_order.clear();
        drop(processed);
        drop(access_order);
        if let Some(ref path) = self.persist_path {
            let _ = std::fs::remove_file(path);
        }
    }

    fn update_access_order(&self, key: &str) {
        let mut access_order = self.access_order.lock();

        if let Some(pos) = access_order.iter().position(|k| k == key) {
            access_order.remove(pos);
        }

        access_order.push_front(key.to_string());
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
