use std::collections::HashMap;
use parking_lot::RwLock;
use feature::FEATURE_SCHEMA_VERSION;

#[derive(Debug, Clone)]
pub struct ModelInfo {
    pub model_id: String,
    pub version: u32,
    pub input_features: Vec<String>,
    pub applicable_regimes: Vec<i32>,
    pub priority: u32,
    /// Feature schema version the model was trained with
    pub feature_schema_version: u32,
}

/// Error returned when model schema doesn't match the engine
#[derive(Debug, Clone)]
pub struct SchemaMismatchError {
    pub model_id: String,
    pub model_schema_version: u32,
    pub engine_schema_version: u32,
}

impl std::fmt::Display for SchemaMismatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Schema mismatch for model '{}': model version {} != engine version {}",
            self.model_id, self.model_schema_version, self.engine_schema_version
        )
    }
}

impl std::error::Error for SchemaMismatchError {}

pub struct ModelRegistry {
    models: RwLock<HashMap<String, ModelInfo>>,
    active_model: RwLock<String>,
    shadow_model: RwLock<Option<String>>,
}

impl ModelRegistry {
    pub fn new() -> Self {
        Self {
            models: RwLock::new(HashMap::new()),
            active_model: RwLock::new(String::new()),
            shadow_model: RwLock::new(None),
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

    pub fn hot_swap(&self, new_model: ModelInfo) -> Result<(), SchemaMismatchError> {
        // Validate schema version before swapping
        if new_model.feature_schema_version != FEATURE_SCHEMA_VERSION {
            let err = SchemaMismatchError {
                model_id: new_model.model_id.clone(),
                model_schema_version: new_model.feature_schema_version,
                engine_schema_version: FEATURE_SCHEMA_VERSION,
            };
            tracing::error!(
                model_id = %err.model_id,
                model_version = %err.model_schema_version,
                engine_version = %err.engine_schema_version,
                "Refusing to load model due to schema version mismatch"
            );
            return Err(err);
        }
        self.register(new_model.clone());
        self.set_active(&new_model.model_id);
        Ok(())
    }

    /// Validate that a model's schema version matches the engine's current version
    pub fn validate_schema(&self, model: &ModelInfo) -> Result<(), SchemaMismatchError> {
        if model.feature_schema_version != FEATURE_SCHEMA_VERSION {
            Err(SchemaMismatchError {
                model_id: model.model_id.clone(),
                model_schema_version: model.feature_schema_version,
                engine_schema_version: FEATURE_SCHEMA_VERSION,
            })
        } else {
            Ok(())
        }
    }

    /// Set a shadow model by ID. The model must already be registered.
    /// Returns None if the model exists, or the model_id back if not found.
    pub fn set_shadow(&self, model_id: &str) -> Option<String> {
        let models = self.models.read();
        if !models.contains_key(model_id) {
            return Some(model_id.to_string());
        }
        drop(models);
        let mut shadow = self.shadow_model.write();
        *shadow = Some(model_id.to_string());
        None
    }

    /// Get the shadow model info, if one is set
    pub fn get_shadow(&self) -> Option<ModelInfo> {
        let shadow = self.shadow_model.read();
        if let Some(ref id) = *shadow {
            let models = self.models.read();
            models.get(id).cloned()
        } else {
            None
        }
    }

    /// Check if a shadow model is currently loaded
    pub fn has_shadow(&self) -> bool {
        self.shadow_model.read().is_some()
    }

    /// Clear the shadow model slot
    pub fn clear_shadow(&self) {
        let mut shadow = self.shadow_model.write();
        *shadow = None;
    }

    /// Promote the shadow model to active. Validates schema before promoting.
    /// Returns Err if no shadow model is set, or if schema validation fails.
    pub fn promote_shadow(&self) -> Result<(), PromotionError> {
        let shadow_id = {
            let shadow = self.shadow_model.read();
            shadow.clone()
        };
        let shadow_id = match shadow_id {
            Some(id) => id,
            None => return Err(PromotionError::NoShadowModel),
        };
        let model_info = {
            let models = self.models.read();
            models.get(&shadow_id).cloned()
        };
        let model_info = match model_info {
            Some(info) => info,
            None => return Err(PromotionError::NoShadowModel),
        };
        if model_info.feature_schema_version != FEATURE_SCHEMA_VERSION {
            return Err(PromotionError::SchemaMismatch(SchemaMismatchError {
                model_id: model_info.model_id.clone(),
                model_schema_version: model_info.feature_schema_version,
                engine_schema_version: FEATURE_SCHEMA_VERSION,
            }));
        }
        self.set_active(&shadow_id);
        let mut shadow = self.shadow_model.write();
        *shadow = None;
        tracing::info!(shadow_model_id = %shadow_id, "Shadow model promoted to active");
        Ok(())
    }
}

#[derive(Debug)]
pub enum PromotionError {
    NoShadowModel,
    SchemaMismatch(SchemaMismatchError),
}

impl std::fmt::Display for PromotionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PromotionError::NoShadowModel => write!(f, "No shadow model is set"),
            PromotionError::SchemaMismatch(err) => write!(f, "{}", err),
        }
    }
}

impl std::error::Error for PromotionError {}

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
            feature_schema_version: FEATURE_SCHEMA_VERSION,
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
            feature_schema_version: FEATURE_SCHEMA_VERSION,
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
            feature_schema_version: FEATURE_SCHEMA_VERSION,
        };
        registry.register(info1);
        registry.set_active("v1");

        let info2 = ModelInfo {
            model_id: "v2".to_string(),
            version: 2,
            input_features: vec![],
            applicable_regimes: vec![],
            priority: 2,
            feature_schema_version: FEATURE_SCHEMA_VERSION,
        };
        let result = registry.hot_swap(info2);
        assert!(result.is_ok());

        let active = registry.get_active().unwrap();
        assert_eq!(active.model_id, "v2");
        assert_eq!(active.version, 2);
    }

    #[test]
    fn test_schema_mismatch_rejected() {
        let registry = ModelRegistry::new();
        let mismatched_model = ModelInfo {
            model_id: "v999".to_string(),
            version: 1,
            input_features: vec![],
            applicable_regimes: vec![],
            priority: 1,
            feature_schema_version: 999, // Wrong version
        };

        let result = registry.hot_swap(mismatched_model);
        assert!(result.is_err());
        
        // Active model should not have changed
        let active = registry.get_active();
        assert!(active.is_none());
    }

    #[test]
    fn test_shadow_model_set_and_get() {
        let registry = ModelRegistry::new();
        let info = ModelInfo {
            model_id: "v2".to_string(),
            version: 2,
            input_features: vec![],
            applicable_regimes: vec![],
            priority: 2,
            feature_schema_version: FEATURE_SCHEMA_VERSION,
        };
        registry.register(info);
        assert!(!registry.has_shadow());
        let err = registry.set_shadow("v2");
        assert!(err.is_none());
        assert!(registry.has_shadow());
        let shadow = registry.get_shadow().unwrap();
        assert_eq!(shadow.model_id, "v2");
        assert_eq!(shadow.version, 2);
    }

    #[test]
    fn test_shadow_model_not_registered() {
        let registry = ModelRegistry::new();
        let err = registry.set_shadow("nonexistent");
        assert_eq!(err, Some("nonexistent".to_string()));
    }

    #[test]
    fn test_shadow_model_clear() {
        let registry = ModelRegistry::new();
        let info = ModelInfo {
            model_id: "v2".to_string(),
            version: 2,
            input_features: vec![],
            applicable_regimes: vec![],
            priority: 2,
            feature_schema_version: FEATURE_SCHEMA_VERSION,
        };
        registry.register(info);
        registry.set_shadow("v2");
        assert!(registry.has_shadow());
        registry.clear_shadow();
        assert!(!registry.has_shadow());
    }

    #[test]
    fn test_shadow_promotion() {
        let registry = ModelRegistry::new();
        let info1 = ModelInfo {
            model_id: "v1".to_string(),
            version: 1,
            input_features: vec![],
            applicable_regimes: vec![],
            priority: 1,
            feature_schema_version: FEATURE_SCHEMA_VERSION,
        };
        registry.register(info1);
        registry.set_active("v1");

        let info2 = ModelInfo {
            model_id: "v2".to_string(),
            version: 2,
            input_features: vec![],
            applicable_regimes: vec![],
            priority: 2,
            feature_schema_version: FEATURE_SCHEMA_VERSION,
        };
        registry.register(info2);
        assert!(registry.set_shadow("v2").is_none());

        let result = registry.promote_shadow();
        assert!(result.is_ok());

        let active = registry.get_active().unwrap();
        assert_eq!(active.model_id, "v2");
        assert!(!registry.has_shadow());
    }

    #[test]
    fn test_shadow_promotion_no_shadow() {
        let registry = ModelRegistry::new();
        let result = registry.promote_shadow();
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_schema() {
        let registry = ModelRegistry::new();
        
        let valid_model = ModelInfo {
            model_id: "v1".to_string(),
            version: 1,
            input_features: vec![],
            applicable_regimes: vec![],
            priority: 1,
            feature_schema_version: FEATURE_SCHEMA_VERSION,
        };
        assert!(registry.validate_schema(&valid_model).is_ok());

        let invalid_model = ModelInfo {
            model_id: "v2".to_string(),
            version: 1,
            input_features: vec![],
            applicable_regimes: vec![],
            priority: 1,
            feature_schema_version: 999,
        };
        let result = registry.validate_schema(&invalid_model);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.model_schema_version, 999);
        assert_eq!(err.engine_schema_version, FEATURE_SCHEMA_VERSION);
    }
}
