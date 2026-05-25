use std::collections::HashMap;
use parking_lot::RwLock;

#[derive(Debug, Clone)]
pub struct ModelInfo {
    pub model_id: String,
    pub version: u32,
    pub input_features: Vec<String>,
    pub applicable_regimes: Vec<i32>,
    pub priority: u32,
}

pub struct ModelRegistry {
    models: RwLock<HashMap<String, ModelInfo>>,
    active_model: RwLock<String>,
}

impl ModelRegistry {
    pub fn new() -> Self {
        Self {
            models: RwLock::new(HashMap::new()),
            active_model: RwLock::new(String::new()),
        }
    }

    pub fn register(&self, info: ModelInfo) {
        let mut models = self.models.write();
        models.insert(info.model_id.clone(), info);
    }

    pub fn set_active(&self, model_id: &str) {
        let mut active = self.active_model.write();
        *active = model_id.to_string();
    }

    pub fn get_active(&self) -> Option<ModelInfo> {
        let active = self.active_model.read();
        if active.is_empty() {
            return None;
        }
        let models = self.models.read();
        models.get(&*active).cloned()
    }

    pub fn list_models(&self) -> Vec<ModelInfo> {
        let models = self.models.read();
        models.values().cloned().collect()
    }

    pub fn hot_swap(&self, new_model: ModelInfo) {
        self.register(new_model.clone());
        self.set_active(&new_model.model_id);
    }
}

impl Default for ModelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_model_registry_register_and_list() {
        let registry = ModelRegistry::new();
        let info = ModelInfo {
            model_id: "v1".to_string(),
            version: 1,
            input_features: vec!["mid_price".to_string()],
            applicable_regimes: vec![0, 1],
            priority: 1,
        };
        registry.register(info);
        let models = registry.list_models();
        assert_eq!(models.len(), 1);
    }

    #[test]
    fn test_model_registry_active_model() {
        let registry = ModelRegistry::new();
        let info = ModelInfo {
            model_id: "v1".to_string(),
            version: 1,
            input_features: vec!["mid_price".to_string()],
            applicable_regimes: vec![0],
            priority: 1,
        };
        registry.register(info.clone());
        registry.set_active("v1");
        let active = registry.get_active().unwrap();
        assert_eq!(active.model_id, "v1");
    }

    #[test]
    fn test_model_registry_hot_swap() {
        let registry = ModelRegistry::new();
        let info1 = ModelInfo {
            model_id: "v1".to_string(),
            version: 1,
            input_features: vec![],
            applicable_regimes: vec![],
            priority: 1,
        };
        registry.register(info1);
        registry.set_active("v1");

        let info2 = ModelInfo {
            model_id: "v2".to_string(),
            version: 2,
            input_features: vec![],
            applicable_regimes: vec![],
            priority: 2,
        };
        registry.hot_swap(info2);

        let active = registry.get_active().unwrap();
        assert_eq!(active.model_id, "v2");
        assert_eq!(active.version, 2);
    }
}
