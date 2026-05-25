use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CooldownKey {
    pub symbol: String,
    pub intent_type: String,
}

pub struct CooldownTracker {
    cooldowns: HashMap<CooldownKey, u64>,
    entry_cooldown_ms: u64,
    exit_cooldown_ms: u64,
}

impl CooldownTracker {
    pub fn new(entry_cooldown_ms: u64, exit_cooldown_ms: u64) -> Self {
        Self {
            cooldowns: HashMap::new(),
            entry_cooldown_ms,
            exit_cooldown_ms,
        }
    }

    pub fn can_act(&self, symbol: &str, intent_type: &str) -> bool {
        let key = CooldownKey {
            symbol: symbol.to_string(),
            intent_type: intent_type.to_string(),
        };

        if let Some(&last_time) = self.cooldowns.get(&key) {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let cooldown = if intent_type == "entry" || intent_type == "scale_in" {
                self.entry_cooldown_ms
            } else {
                self.exit_cooldown_ms
            };
            now.saturating_sub(last_time) > cooldown
        } else {
            true
        }
    }

    pub fn record_action(&mut self, symbol: &str, intent_type: &str) {
        let key = CooldownKey {
            symbol: symbol.to_string(),
            intent_type: intent_type.to_string(),
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.cooldowns.insert(key, now);
    }

    pub fn reset(&mut self) {
        self.cooldowns.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cooldown_can_act_initially() {
        let tracker = CooldownTracker::new(5000, 2000);
        assert!(tracker.can_act("AAPL", "entry"));
    }

    #[test]
    fn test_cooldown_blocks_after_action() {
        let mut tracker = CooldownTracker::new(5000, 2000);
        tracker.record_action("AAPL", "entry");
        assert!(!tracker.can_act("AAPL", "entry"));
    }

    #[test]
    fn test_cooldown_different_symbols_independent() {
        let mut tracker = CooldownTracker::new(5000, 2000);
        tracker.record_action("AAPL", "entry");
        assert!(!tracker.can_act("AAPL", "entry"));
        assert!(tracker.can_act("MSFT", "entry"));
    }

    #[test]
    fn test_cooldown_different_types_independent() {
        let mut tracker = CooldownTracker::new(5000, 2000);
        tracker.record_action("AAPL", "entry");
        assert!(!tracker.can_act("AAPL", "entry"));
        assert!(tracker.can_act("AAPL", "exit"));
    }

    #[test]
    fn test_cooldown_reset() {
        let mut tracker = CooldownTracker::new(5000, 2000);
        tracker.record_action("AAPL", "entry");
        tracker.reset();
        assert!(tracker.can_act("AAPL", "entry"));
    }
}
