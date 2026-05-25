use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use arc_swap::ArcSwap;
use parking_lot::RwLock;

use crate::config::EngineConfig;
use crate::kill_switch::KillSwitch;
use crate::metrics::GlobalMetrics;

pub struct SystemState {
    pub kill_switch: KillSwitch,
    pub config: Arc<ArcSwap<EngineConfig>>,
    pub metrics: GlobalMetrics,
    pub running: AtomicBool,
}

impl SystemState {
    pub fn new(config: EngineConfig) -> Self {
        Self {
            kill_switch: KillSwitch::new(),
            config: Arc::new(ArcSwap::new(Arc::new(config))),
            metrics: GlobalMetrics::new(),
            running: AtomicBool::new(true),
        }
    }

    pub fn shutdown(&self) {
        self.kill_switch.activate();
        self.running.store(false, Ordering::SeqCst);
    }

    pub fn is_running(&self) -> bool {
        !self.kill_switch.is_active() && self.running.load(Ordering::Relaxed)
    }

    pub fn update_config(&self, config: EngineConfig) {
        self.config.store(Arc::new(config));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> EngineConfig {
        EngineConfig::default()
    }

    #[test]
    fn test_system_state_new() {
        let state = SystemState::new(default_config());
        assert!(state.is_running());
    }

    #[test]
    fn test_system_state_shutdown() {
        let state = SystemState::new(default_config());
        state.shutdown();
        assert!(!state.is_running());
    }

    #[test]
    fn test_system_state_config_update() {
        let state = SystemState::new(default_config());
        let new_config = EngineConfig {
            max_assets: 4,
            ..default_config()
        };
        state.update_config(new_config);
        let loaded = state.config.load();
        assert_eq!(loaded.max_assets, 4);
    }
}
