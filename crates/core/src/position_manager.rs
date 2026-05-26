//! Deprecated: `PositionManager` has been unified into `PortfolioManager`.
//! This module re-exports aliases for backwards compatibility during migration.

pub use crate::portfolio_manager::PortfolioManager as PositionManager;
pub use crate::portfolio_manager::Position;
