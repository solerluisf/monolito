pub struct RequestValidator;

pub type ValidationResult = Result<(), String>;

impl RequestValidator {
    pub fn new() -> Self {
        Self
    }

    pub fn validate_symbol(symbol: &str) -> ValidationResult {
        if symbol.is_empty() {
            return Err("symbol cannot be empty".to_string());
        }

        if symbol.len() > 20 {
            return Err("symbol too long (max 20 chars)".to_string());
        }

        if !symbol.chars().all(|c| c.is_alphanumeric() || c == '.' || c == '-') {
            return Err("symbol contains invalid characters".to_string());
        }

        Ok(())
    }

    pub fn validate_quantity(qty: f64) -> ValidationResult {
        if qty <= 0.0 {
            return Err("quantity must be greater than 0".to_string());
        }

        if qty > 1_000_000.0 {
            return Err("quantity exceeds maximum (1,000,000)".to_string());
        }

        if !qty.is_finite() {
            return Err("quantity must be a valid number".to_string());
        }

        Ok(())
    }

    pub fn validate_price(price: f64) -> ValidationResult {
        if price <= 0.0 {
            return Err("price must be greater than 0".to_string());
        }

        if !price.is_finite() {
            return Err("price must be a valid number".to_string());
        }

        Ok(())
    }

    pub fn validate_order_id(order_id: &str) -> ValidationResult {
        if order_id.is_empty() {
            return Err("order_id cannot be empty".to_string());
        }

        if order_id.len() > 100 {
            return Err("order_id too long (max 100 chars)".to_string());
        }

        Ok(())
    }

    pub fn validate_notional(notional: f64) -> ValidationResult {
        if notional <= 0.0 {
            return Err("notional must be greater than 0".to_string());
        }

        if !notional.is_finite() {
            return Err("notional must be a valid number".to_string());
        }

        Ok(())
    }
}

impl Default for RequestValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_symbol_valid() {
        assert!(RequestValidator::validate_symbol("AAPL").is_ok());
        assert!(RequestValidator::validate_symbol("BRK-B").is_ok());
        assert!(RequestValidator::validate_symbol("VX.Z").is_ok());
    }

    #[test]
    fn test_validate_symbol_invalid() {
        assert!(RequestValidator::validate_symbol("").is_err());
        assert!(RequestValidator::validate_symbol("A".repeat(21).as_str()).is_err());
        assert!(RequestValidator::validate_symbol("AAPL@").is_err());
        assert!(RequestValidator::validate_symbol("AAP L").is_err());
    }

    #[test]
    fn test_validate_quantity_valid() {
        assert!(RequestValidator::validate_quantity(1.0).is_ok());
        assert!(RequestValidator::validate_quantity(1000.0).is_ok());
    }

    #[test]
    fn test_validate_quantity_invalid() {
        assert!(RequestValidator::validate_quantity(0.0).is_err());
        assert!(RequestValidator::validate_quantity(-1.0).is_err());
        assert!(RequestValidator::validate_quantity(2_000_000.0).is_err());
        assert!(RequestValidator::validate_quantity(f64::NAN).is_err());
    }

    #[test]
    fn test_validate_price_valid() {
        assert!(RequestValidator::validate_price(0.01).is_ok());
        assert!(RequestValidator::validate_price(150.0).is_ok());
    }

    #[test]
    fn test_validate_price_invalid() {
        assert!(RequestValidator::validate_price(0.0).is_err());
        assert!(RequestValidator::validate_price(-10.0).is_err());
        assert!(RequestValidator::validate_price(f64::INFINITY).is_err());
    }

    #[test]
    fn test_validate_order_id() {
        assert!(RequestValidator::validate_order_id("order-123").is_ok());
        assert!(RequestValidator::validate_order_id("").is_err());
    }
}
