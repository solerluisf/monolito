use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
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
}

impl IdempotencyStore {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            processed: Mutex::new(HashMap::with_capacity(capacity)),
            access_order: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
        }
    }

    pub fn is_processed(&self, key: &str) -> bool {
        let mut processed = self.processed.lock().unwrap();

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
        let mut processed = self.processed.lock().unwrap();
        let mut access_order = self.access_order.lock().unwrap();

        if !processed.contains_key(&key) && processed.len() >= self.capacity {
            if let Some(lru_key) = access_order.pop_back() {
                processed.remove(&lru_key);
            }
        }

        let entry = Entry {
            result,
            last_accessed: Instant::now(),
        };

        if processed.contains_key(&key) {
            access_order.retain(|k| k != &key);
        }

        processed.insert(key.clone(), entry);
        access_order.push_front(key);
    }

    pub fn get_result(&self, key: &str) -> Option<String> {
        let mut processed = self.processed.lock().unwrap();

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
        self.processed.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.processed.lock().unwrap().is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn clear(&self) {
        let mut processed = self.processed.lock().unwrap();
        let mut access_order = self.access_order.lock().unwrap();
        processed.clear();
        access_order.clear();
    }

    fn update_access_order(&self, key: &str) {
        let mut access_order = self.access_order.lock().unwrap();

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
}
