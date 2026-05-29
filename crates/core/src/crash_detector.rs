use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::config::EngineConfig;
use crate::clock::wall_time_ns;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CrashState {
    startup_timestamp_ms: u64,
    crash_count: u32,
    first_crash_timestamp_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_known_good_config: Option<EngineConfig>,
}

pub struct CrashDetector {
    state_file: PathBuf,
}

impl CrashDetector {
    pub fn new(state_file: PathBuf) -> Self {
        Self { state_file }
    }

    pub fn check_and_record_startup(&self) -> bool {
        let now = wall_time_ns() / 1_000_000; // Convert ns to ms

        let prev_state = std::fs::read_to_string(&self.state_file)
            .ok()
            .and_then(|s| serde_json::from_str::<CrashState>(&s).ok());

        let (enter_safe_mode, new_state) = if let Some(prev) = prev_state {
            let elapsed_ms = now.saturating_sub(prev.startup_timestamp_ms);
            let total_elapsed_ms = now.saturating_sub(prev.first_crash_timestamp_ms);

            if elapsed_ms < 60_000 && total_elapsed_ms < 300_000 {
                let new_count = prev.crash_count + 1;
                if new_count >= 3 {
                    tracing::error!(
                        crash_count = %new_count,
                        "Crash loop detected ({} crashes within 5 min). Entering safe mode.",
                        new_count
                    );
                    (true, None)
                } else {
                    (false, Some(CrashState {
                        startup_timestamp_ms: now,
                        crash_count: new_count,
                        first_crash_timestamp_ms: prev.first_crash_timestamp_ms,
                        last_known_good_config: prev.last_known_good_config,
                    }))
                }
            } else if elapsed_ms < 60_000 {
                (false, Some(CrashState {
                    startup_timestamp_ms: now,
                    crash_count: 2,
                    first_crash_timestamp_ms: now,
                    last_known_good_config: prev.last_known_good_config,
                }))
            } else {
                (false, Some(CrashState {
                    startup_timestamp_ms: now,
                    crash_count: 1,
                    first_crash_timestamp_ms: now,
                    last_known_good_config: prev.last_known_good_config,
                }))
            }
        } else {
            (false, Some(CrashState {
                startup_timestamp_ms: now,
                crash_count: 1,
                first_crash_timestamp_ms: now,
                last_known_good_config: None,
            }))
        };

        if let Some(s) = new_state {
            if let Ok(json) = serde_json::to_string_pretty(&s) {
                let _ = std::fs::write(&self.state_file, json);
            }
        }

        enter_safe_mode
    }

    pub fn clear(&self) {
        let _ = std::fs::remove_file(&self.state_file);
        let config_path = self.state_file.with_extension("config");
        let _ = std::fs::remove_file(config_path);
    }

    pub fn save_working_config(&self, config: &EngineConfig) {
        if let Ok(json) = serde_json::to_string_pretty(config) {
            let path = self.state_file.with_extension("config");
            let _ = std::fs::write(&path, json);
        }
    }

    pub fn get_last_known_good_config(&self) -> Option<EngineConfig> {
        let path = self.state_file.with_extension("config");
        std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| serde_json::from_str::<EngineConfig>(&s).ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn make_detector() -> (CrashDetector, PathBuf) {
        let tmp = std::env::temp_dir().join(format!(
            "crash_test_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = tmp.join("crash_state.json");
        let _ = fs::create_dir_all(&tmp);
        (CrashDetector::new(path.clone()), path)
    }

    #[test]
    fn test_first_startup_no_safe_mode() {
        let (detector, _path) = make_detector();
        assert!(!detector.check_and_record_startup());
        assert!(_path.exists());
    }

    #[test]
    fn test_rapid_crashes_trigger_safe_mode() {
        let (detector, _path) = make_detector();
        assert!(!detector.check_and_record_startup());
        assert!(!detector.check_and_record_startup());
        assert!(detector.check_and_record_startup());
    }

    #[test]
    fn test_clear_resets_counter() {
        let (detector, _path) = make_detector();
        assert!(!detector.check_and_record_startup());
        detector.clear();
        assert!(!_path.exists());
        assert!(!detector.check_and_record_startup());
    }

    #[test]
    fn test_slow_startup_resets_counter() {
        let (detector, _path) = make_detector();
        assert!(!detector.check_and_record_startup());

        let mut state: CrashState = serde_json::from_str(&fs::read_to_string(&_path).unwrap()).unwrap();
        state.startup_timestamp_ms -= 120_000;
        state.first_crash_timestamp_ms -= 120_000;
        fs::write(&_path, serde_json::to_string(&state).unwrap()).unwrap();

        assert!(!detector.check_and_record_startup());
        let new_state: CrashState = serde_json::from_str(&fs::read_to_string(&_path).unwrap()).unwrap();
        assert_eq!(new_state.crash_count, 1);
    }

    #[test]
    fn test_save_and_load_config() {
        let (detector, _path) = make_detector();
        let config = EngineConfig::default();
        detector.save_working_config(&config);
        let loaded = detector.get_last_known_good_config();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().max_assets, config.max_assets);
    }

    #[test]
    fn test_clear_removes_config_file() {
        let (detector, _path) = make_detector();
        detector.save_working_config(&EngineConfig::default());
        let config_path = _path.with_extension("config");
        assert!(config_path.exists());
        detector.clear();
        assert!(!config_path.exists());
    }
}
