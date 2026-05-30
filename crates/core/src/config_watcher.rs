use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crossbeam_channel::Sender;
use notify::{Watcher, RecursiveMode, RecommendedWatcher, event::EventKind};
use parking_lot::RwLock;
use tracing::{info, warn, error};

use crate::config::{EngineConfig, ConfigValidationError};
use crate::command_channel::ControlCommand;

#[derive(Debug, Clone, PartialEq)]
pub enum ConfigWatcherError {
    Io(String),
    Parse(String),
    Validation(String),
    WatcherSetup(String),
    NoConfigFile,
}

impl fmt::Display for ConfigWatcherError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigWatcherError::Io(msg) => write!(f, "IO error: {}", msg),
            ConfigWatcherError::Parse(msg) => write!(f, "Parse error: {}", msg),
            ConfigWatcherError::Validation(msg) => write!(f, "Validation error: {}", msg),
            ConfigWatcherError::WatcherSetup(msg) => write!(f, "Watcher setup: {}", msg),
            ConfigWatcherError::NoConfigFile => write!(f, "No TOML config file found"),
        }
    }
}

impl std::error::Error for ConfigWatcherError {}

pub struct ConfigWatcher {
    watcher: Option<RecommendedWatcher>,
    config: Arc<RwLock<EngineConfig>>,
    running: Arc<AtomicBool>,
    reload_count: Arc<AtomicU64>,
    /// Number of reload attempts that were rejected by validation.
    rejected_count: Arc<AtomicU64>,
    /// Optional sender to route reload events through the command channel
    /// so they follow the same propagation path as API-triggered config changes.
    command_tx: Option<Sender<ControlCommand>>,
}

impl ConfigWatcher {
    pub fn new(config: Arc<RwLock<EngineConfig>>) -> Self {
        Self {
            watcher: None,
            config,
            running: Arc::new(AtomicBool::new(true)),
            reload_count: Arc::new(AtomicU64::new(0)),
            rejected_count: Arc::new(AtomicU64::new(0)),
            command_tx: None,
        }
    }

    pub fn set_command_channel(&mut self, tx: Sender<ControlCommand>) {
        self.command_tx = Some(tx);
    }

    #[tracing::instrument(skip(self), fields(path = %path))]
    pub fn start(&mut self, path: &str) -> Result<(), ConfigWatcherError> {
        let config = Arc::clone(&self.config);
        let running = Arc::clone(&self.running);
        let reload_count = Arc::clone(&self.reload_count);
        let rejected_count = Arc::clone(&self.rejected_count);
        let command_tx = self.command_tx.clone();

        let path_buf = std::path::PathBuf::from(path);
        if !path_buf.exists() {
            info!(path = %path, "Config path does not exist, skipping watcher");
            return Ok(());
        }

        let mut watcher = RecommendedWatcher::new(
            move |res: Result<notify::Event, notify::Error>| {
                if let Ok(event) = res {
                    if !running.load(Ordering::Relaxed) {
                        return;
                    }

                    if matches!(event.kind, EventKind::Modify(_)) {
                        info!(?event.paths, "Config file changed, reloading");
                        match Self::reload_config(&config, &event.paths) {
                            Ok(_) => {
                                reload_count.fetch_add(1, Ordering::Relaxed);
                                // Route through command channel so hot-swappable
                                // params are propagated to live components
                                // (same path as API-triggered config changes).
                                if let Some(ref tx) = command_tx {
                                    if let Err(e) = tx.try_send(ControlCommand::ReloadConfig) {
                                        warn!(error = %e, "Failed to send ReloadConfig to command channel");
                                    }
                                }
                                info!("Config reloaded successfully");
                            }
                            Err(e) => {
                                if matches!(e, ConfigWatcherError::Validation(_)) {
                                    rejected_count.fetch_add(1, Ordering::Relaxed);
                                    error!("Rejected invalid config — keeping previous config: {}", e);
                                } else {
                                    warn!("Failed to reload config: {}", e);
                                }
                            }
                        }
                    }
                }
            },
            notify::Config::default(),
        ).map_err(|e| ConfigWatcherError::WatcherSetup(e.to_string()))?;

        watcher
            .watch(&path_buf, RecursiveMode::Recursive)
            .map_err(|e| ConfigWatcherError::WatcherSetup(e.to_string()))?;

        info!(path = %path, "Config watcher started (with sandbox validation)");
        self.watcher = Some(watcher);
        Ok(())
    }

    fn reload_config(config: &Arc<RwLock<EngineConfig>>, paths: &[std::path::PathBuf]) -> Result<(), ConfigWatcherError> {
        for path in paths {
            if path.extension().map_or(false, |ext| ext == "toml") {
                let content = std::fs::read_to_string(path)
                    .map_err(|e| ConfigWatcherError::Io(format!("Failed to read {}: {}", path.display(), e)))?;
                let new_config: EngineConfig = toml::from_str(&content)
                    .map_err(|e| ConfigWatcherError::Parse(format!("Failed to parse {}: {}", path.display(), e)))?;

                new_config.validate()
                    .map_err(|e: ConfigValidationError| ConfigWatcherError::Validation(format!("validation failed: {}", e)))?;

                let mut current = config.write();
                *current = new_config;
                return Ok(());
            }
        }
        Err(ConfigWatcherError::NoConfigFile)
    }

    pub fn reload_count(&self) -> u64 {
        self.reload_count.load(Ordering::Relaxed)
    }

    /// Number of reload attempts that were rejected because the parsed
    /// config failed `validate()`.
    pub fn rejected_count(&self) -> u64 {
        self.rejected_count.load(Ordering::Relaxed)
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        self.watcher = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::time::Duration;

    #[test]
    fn test_config_watcher_reload() {
        let config = Arc::new(RwLock::new(EngineConfig::default()));
        let mut watcher = ConfigWatcher::new(Arc::clone(&config));

        let tmp_dir = std::env::temp_dir().join(format!("config_test_{}", std::process::id()));
        std::fs::create_dir_all(&tmp_dir).ok();
        let config_path = tmp_dir.join("engine.toml");

        let initial = toml::to_string(&EngineConfig::default()).unwrap();
        let mut file = std::fs::File::create(&config_path).unwrap();
        file.write_all(initial.as_bytes()).unwrap();
        drop(file);

        std::thread::sleep(Duration::from_millis(100));
        watcher.start(config_path.to_str().unwrap()).unwrap();

        let mut new_config = EngineConfig::default();
        new_config.max_assets = 8;
        let updated = toml::to_string(&new_config).unwrap();
        std::fs::write(&config_path, updated).unwrap();

        std::thread::sleep(Duration::from_millis(500));

        assert!(watcher.reload_count() > 0 || config.read().max_assets == 8);

        watcher.stop();
        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn test_config_watcher_nonexistent_path() {
        let config = Arc::new(RwLock::new(EngineConfig::default()));
        let mut watcher = ConfigWatcher::new(config);
        let result = watcher.start("/nonexistent/path");
        assert!(result.is_ok());
    }

    /// Acceptance test: write an invalid config (negative leverage) to disk,
    /// verify that reload is rejected and the previous valid config is retained.
    #[test]
    fn test_config_watcher_rejects_invalid_config() {
        // Start with a known-valid config
        let original = EngineConfig::default();
        let config = Arc::new(RwLock::new(original.clone()));

        let tmp_dir = std::env::temp_dir().join(format!("config_validate_test_{}", std::process::id()));
        std::fs::create_dir_all(&tmp_dir).ok();
        let config_path = tmp_dir.join("invalid.toml");

        // Write valid initial config
        let valid_toml = toml::to_string(&original).unwrap();
        std::fs::write(&config_path, &valid_toml).unwrap();

        // Start watcher
        let mut watcher = ConfigWatcher::new(Arc::clone(&config));
        watcher.start(config_path.to_str().unwrap()).unwrap();
        std::thread::sleep(Duration::from_millis(150));

        // Write invalid config: negative leverage
        let mut invalid = EngineConfig::default();
        invalid.risk_config.max_leverage = -3.0; // invalid
        invalid.max_assets = 99;                  // also inconsistent with asset_configs
        let invalid_toml = toml::to_string(&invalid).unwrap();
        std::fs::write(&config_path, &invalid_toml).unwrap();

        // Give the watcher time to pick up the change
        std::thread::sleep(Duration::from_millis(500));

        // The original config must still be in place — no swap happened
        let current = config.read();
        assert_eq!(
            current.risk_config.max_leverage, original.risk_config.max_leverage,
            "leverage should remain at original value after rejected reload"
        );
        assert_eq!(
            current.max_assets, original.max_assets,
            "max_assets should remain at original value after rejected reload"
        );

        // Reload count should NOT have incremented for this attempt
        // (or at minimum, rejected_count should reflect the rejection)
        assert!(
            watcher.rejected_count() >= 1 || watcher.reload_count() == 0,
            "expected rejection to be counted or no successful reload"
        );

        watcher.stop();
        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    /// Direct unit test of `reload_config` with an invalid TOML — no
    /// filesystem watcher needed.
    #[test]
    fn test_reload_config_rejects_negative_leverage_directly() {
        let original = EngineConfig::default();
        let config = Arc::new(RwLock::new(original.clone()));

        let tmp_dir = std::env::temp_dir().join(format!("direct_validate_{}", std::process::id()));
        std::fs::create_dir_all(&tmp_dir).ok();
        let path = tmp_dir.join("bad.toml");

        let mut bad = EngineConfig::default();
        bad.risk_config.max_leverage = -1.0;
        std::fs::write(&path, toml::to_string(&bad).unwrap()).unwrap();

        let result = ConfigWatcher::reload_config(&config, &[path.clone()]);
        assert!(result.is_err(), "reload should fail on negative leverage");
        match result.unwrap_err() {
            ConfigWatcherError::Validation(msg) => assert!(msg.contains("validation failed")),
            _ => panic!("expected Validation error"),
        }

        // Verify original is untouched
        assert_eq!(config.read().risk_config.max_leverage, original.risk_config.max_leverage);

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }
}
