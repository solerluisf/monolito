use std::fmt;
use crate::config::ValidatorConfig;

#[derive(Debug, Clone, PartialEq)]
pub enum ValidationError {
    EmptyField(&'static str),
    TooLong(&'static str, usize),
    InvalidCharacters(&'static str),
    OutOfRange(&'static str),
    InvalidNumber(&'static str),
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationError::EmptyField(field) => write!(f, "{} cannot be empty", field),
            ValidationError::TooLong(field, max) => write!(f, "{} too long (max {} chars)", field, max),
            ValidationError::InvalidCharacters(field) => write!(f, "{} contains invalid characters", field),
            ValidationError::OutOfRange(field) => write!(f, "{} out of range", field),
            ValidationError::InvalidNumber(field) => write!(f, "{} must be a valid number", field),
        }
    }
}

impl std::error::Error for ValidationError {}

pub type ValidationResult = Result<(), ValidationError>;

pub struct RequestValidator {
    pub config: ValidatorConfig,
}

impl RequestValidator {
    pub fn new(config: ValidatorConfig) -> Self {
        Self { config }
    }

    pub fn validate_symbol(&self, symbol: &str) -> ValidationResult {
        if symbol.is_empty() {
            return Err(ValidationError::EmptyField("symbol"));
        }

        if symbol.len() > self.config.max_symbol_length {
            return Err(ValidationError::TooLong("symbol", self.config.max_symbol_length));
        }

        if !symbol.chars().all(|c| c.is_alphanumeric() || c == '.' || c == '-') {
            return Err(ValidationError::InvalidCharacters("symbol"));
        }

        Ok(())
    }

    pub fn validate_quantity(&self, qty: f64) -> ValidationResult {
        if qty <= 0.0 {
            return Err(ValidationError::OutOfRange("quantity"));
        }

        if qty > self.config.max_quantity {
            return Err(ValidationError::TooLong("quantity", self.config.max_quantity as usize));
        }

        if !qty.is_finite() {
            return Err(ValidationError::InvalidNumber("quantity"));
        }

        Ok(())
    }

    pub fn validate_price(&self, price: f64) -> ValidationResult {
        if price <= 0.0 {
            return Err(ValidationError::OutOfRange("price"));
        }

        if !price.is_finite() {
            return Err(ValidationError::InvalidNumber("price"));
        }

        Ok(())
    }

    pub fn validate_order_id(&self, order_id: &str) -> ValidationResult {
        if order_id.is_empty() {
            return Err(ValidationError::EmptyField("order_id"));
        }

        if order_id.len() > self.config.max_order_id_length {
            return Err(ValidationError::TooLong("order_id", self.config.max_order_id_length));
        }

        Ok(())
    }

    pub fn validate_notional(&self, notional: f64) -> ValidationResult {
        if notional <= 0.0 {
            return Err(ValidationError::OutOfRange("notional"));
        }

        if !notional.is_finite() {
            return Err(ValidationError::InvalidNumber("notional"));
        }

        Ok(())
    }
}

impl Default for RequestValidator {
    fn default() -> Self {
        Self::new(ValidatorConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_validator() -> RequestValidator {
        RequestValidator::new(ValidatorConfig::default())
    }

    #[test]
    fn test_validate_symbol_valid() {
        let v = make_validator();
        assert!(v.validate_symbol("AAPL").is_ok());
        assert!(v.validate_symbol("BRK-B").is_ok());
        assert!(v.validate_symbol("VX.Z").is_ok());
    }

    #[test]
    fn test_validate_symbol_invalid() {
        let v = make_validator();
        assert!(v.validate_symbol("").is_err());
        assert!(v.validate_symbol("A".repeat(21).as_str()).is_err());
        assert!(v.validate_symbol("AAPL@").is_err());
        assert!(v.validate_symbol("AAP L").is_err());
    }

    #[test]
    fn test_validate_quantity_valid() {
        let v = make_validator();
        assert!(v.validate_quantity(1.0).is_ok());
        assert!(v.validate_quantity(1000.0).is_ok());
    }

    #[test]
    fn test_validate_quantity_invalid() {
        let v = make_validator();
        assert!(v.validate_quantity(0.0).is_err());
        assert!(v.validate_quantity(-1.0).is_err());
        assert!(v.validate_quantity(2_000_000.0).is_err());
        assert!(v.validate_quantity(f64::NAN).is_err());
    }

    #[test]
    fn test_validate_price_valid() {
        let v = make_validator();
        assert!(v.validate_price(0.01).is_ok());
        assert!(v.validate_price(150.0).is_ok());
    }

    #[test]
    fn test_validate_price_invalid() {
        let v = make_validator();
        assert!(v.validate_price(0.0).is_err());
        assert!(v.validate_price(-10.0).is_err());
        assert!(v.validate_price(f64::INFINITY).is_err());
    }

    #[test]
    fn test_validate_order_id() {
        let v = make_validator();
        assert!(v.validate_order_id("order-123").is_ok());
        assert!(v.validate_order_id("").is_err());
    }
}
