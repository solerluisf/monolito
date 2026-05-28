use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use serde::{Serialize, Deserialize};

pub const MAX_SYMBOLS: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SymbolId(u16);

impl SymbolId {
    pub const fn from_raw(id: u16) -> Self {
        Self(id)
    }

    pub const fn as_u16(&self) -> u16 {
        self.0
    }

    pub const fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

impl fmt::Display for SymbolId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SymbolId({})", self.0)
    }
}

static INTENT_COUNTER: AtomicU64 = AtomicU64::new(1);
static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);
static TRACE_COUNTER: AtomicU64 = AtomicU64::new(1);

pub fn next_intent_id() -> u64 {
    INTENT_COUNTER.fetch_add(1, Ordering::Relaxed)
}

pub fn next_request_id() -> u64 {
    REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Generates a unique trace_id for causal tracing across the pipeline.
/// Each RawTick gets a trace_id that propagates through all subsequent stages.
pub fn next_trace_id() -> u64 {
    TRACE_COUNTER.fetch_add(1, Ordering::Relaxed)
}

pub struct SymbolRegistry {
    symbol_to_id: HashMap<String, SymbolId>,
    id_to_symbol: Vec<Option<String>>,
    next_id: u16,
}

impl SymbolRegistry {
    pub fn new() -> Self {
        Self {
            symbol_to_id: HashMap::with_capacity(128),
            id_to_symbol: vec![None; MAX_SYMBOLS],
            next_id: 0,
        }
    }

    pub fn register(&mut self, symbol: &str) -> Option<SymbolId> {
        if let Some(&id) = self.symbol_to_id.get(symbol) {
            return Some(id);
        }

        if self.next_id as usize >= MAX_SYMBOLS {
            tracing::error!("Symbol registry full, cannot register '{}'", symbol);
            return None;
        }

        let id = SymbolId(self.next_id);
        self.next_id += 1;

        self.symbol_to_id.insert(symbol.to_string(), id);
        self.id_to_symbol[id.as_usize()] = Some(symbol.to_string());

        tracing::debug!("Registered symbol '{}' with ID {:?}", symbol, id);
        Some(id)
    }

    pub fn lookup(&self, symbol: &str) -> Option<SymbolId> {
        self.symbol_to_id.get(symbol).copied()
    }

    pub fn get_symbol(&self, id: SymbolId) -> Option<&str> {
        self.id_to_symbol.get(id.as_usize())
            .and_then(|s| s.as_deref())
    }

    pub fn count(&self) -> usize {
        self.symbol_to_id.len()
    }

    pub fn is_registered(&self, symbol: &str) -> bool {
        self.symbol_to_id.contains_key(symbol)
    }

    pub fn all_symbols(&self) -> Vec<&str> {
        self.symbol_to_id.keys().map(|s| s.as_str()).collect()
    }
}

impl Default for SymbolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

pub struct SymbolIdArray<T> {
    data: Vec<Option<T>>,
}

impl<T: Clone> SymbolIdArray<T> {
    pub fn new() -> Self {
        Self {
            data: vec![None; MAX_SYMBOLS],
        }
    }

    pub fn get(&self, id: SymbolId) -> Option<&T> {
        self.data.get(id.as_usize()).and_then(|v| v.as_ref())
    }

    pub fn set(&mut self, id: SymbolId, value: T) {
        if let Some(slot) = self.data.get_mut(id.as_usize()) {
            *slot = Some(value);
        }
    }

    pub fn remove(&mut self, id: SymbolId) -> Option<T> {
        self.data.get_mut(id.as_usize()).and_then(|v| v.take())
    }

    pub fn clear(&mut self) {
        for slot in self.data.iter_mut() {
            *slot = None;
        }
    }
}

impl<T: Clone> Default for SymbolIdArray<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_symbol_registry_register_and_lookup() {
        let mut registry = SymbolRegistry::new();
        let aapl_id = registry.register("AAPL").unwrap();
        let msft_id = registry.register("MSFT").unwrap();

        assert_eq!(registry.lookup("AAPL"), Some(aapl_id));
        assert_eq!(registry.lookup("MSFT"), Some(msft_id));
        assert_eq!(registry.lookup("GOOGL"), None);
        assert_eq!(registry.count(), 2);
    }

    #[test]
    fn test_symbol_registry_reverse_lookup() {
        let mut registry = SymbolRegistry::new();
        let aapl_id = registry.register("AAPL").unwrap();

        assert_eq!(registry.get_symbol(aapl_id), Some("AAPL"));
    }

    #[test]
    fn test_symbol_registry_duplicate_register() {
        let mut registry = SymbolRegistry::new();
        let id1 = registry.register("AAPL").unwrap();
        let id2 = registry.register("AAPL").unwrap();

        assert_eq!(id1, id2);
        assert_eq!(registry.count(), 1);
    }

    #[test]
    fn test_symbol_id_array() {
        let mut registry = SymbolRegistry::new();
        let aapl_id = registry.register("AAPL").unwrap();

        let mut array = SymbolIdArray::new();
        array.set(aapl_id, 100);
        assert_eq!(array.get(aapl_id), Some(&100));
        assert_eq!(array.remove(aapl_id), Some(100));
        assert_eq!(array.get(aapl_id), None);
    }

    #[test]
    fn test_symbol_id_array_clear() {
        let mut registry = SymbolRegistry::new();
        let aapl_id = registry.register("AAPL").unwrap();
        let msft_id = registry.register("MSFT").unwrap();

        let mut array = SymbolIdArray::new();
        array.set(aapl_id, 100);
        array.set(msft_id, 200);

        array.clear();
        assert_eq!(array.get(aapl_id), None);
        assert_eq!(array.get(msft_id), None);
    }
}
