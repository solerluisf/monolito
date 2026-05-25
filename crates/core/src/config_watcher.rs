use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use notify::{Watcher, RecursiveMode, RecommendedWatcher, event::EventKind};
use parking_lot::RwLock;
use tracing::{info, warn, error};

use crate::config::EngineConfig;

pub struct ConfigWatcher {
    watcher: Option<RecommendedWatcher>,
    config: Arc<RwLock<EngineConfig>>,
    running: Arc<AtomicBool>,
    reload_count: Arc<std::sync::atomic::AtomicU64>,
}

impl ConfigWatcher {
    pub fn new(config: Arc<RwLock<EngineConfig>>) -> Self {
        Self {
            watcher: None,
            config,
            running: Arc::new(AtomicBool::new(true)),
            reload_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    #[tracing::instrument(skip(self), fields(path = %path))]
    pub fn start(&mut self, path: &str) -> Result<(), String> {
        let config = Arc::clone(&self.config);
        let running = Arc::clone(&self.running);
        let reload_count = Arc::clone(&self.reload_count);

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
                                info!("Config reloaded successfully");
                            }
                            Err(e) => {
                                warn!("Failed to reload config: {}", e);
                            }
                        }
                    }
                }
            },
            notify::Config::default(),
        ).map_err(|e| e.to_string())?;

        watcher
            .watch(&path_buf, RecursiveMode::Recursive)
            .map_err(|e| e.to_string())?;

        info!(path = %path, "Config watcher started");
        self.watcher = Some(watcher);
        Ok(())
    }

    fn reload_config(config: &Arc<RwLock<EngineConfig>>, paths: &[std::path::PathBuf]) -> Result<(), String> {
        for path in paths {
            if path.extension().map_or(false, |ext| ext == "toml") {
                let content = std::fs::read_to_string(path)
                    .map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;
                let new_config: EngineConfig = toml::from_str(&content)
                    .map_err(|e| format!("Failed to parse {}: {}", path.display(), e))?;

                let mut current = config.write();
                *current = new_config;
                return Ok(());
            }
        }
        Err("No TOML config file found in event paths".to_string())
    }

    pub fn reload_count(&self) -> u64 {
        self.reload_count.load(Ordering::Relaxed)
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
}
