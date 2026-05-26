pub mod feature_engine;
pub mod window_manager;
pub mod computers;

pub use feature_engine::{FeatureVector, FeatureSnapshot, RegimeLabel, FeatureIndex, FEATURE_COUNT};
pub use window_manager::WindowManager;
pub use computers::{
    PriceComputer, MicrostructureComputer, MomentumComputer,
    VolatilityComputer, VolumeComputer, RegimeComputer, FeatureEngine,
};
