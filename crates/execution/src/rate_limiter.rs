use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct TokenBucket {
    pub tokens: f64,
    pub max_tokens: f64,
    pub refill_rate: f64,
    pub last_refill_ns: u64,
}

impl TokenBucket {
    pub fn new(max_tokens: f64, refill_rate: f64) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        Self {
            tokens: max_tokens,
            max_tokens,
            refill_rate,
            last_refill_ns: now,
        }
    }

    #[tracing::instrument(skip(self), fields(tokens_before = self.tokens))]
    pub fn try_consume(&mut self, count: f64) -> bool {
        self.refill();
        if self.tokens >= count {
            self.tokens -= count;
            true
        } else {
            false
        }
    }

    fn refill(&mut self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let elapsed_s = (now - self.last_refill_ns) as f64 / 1_000_000_000.0;
        self.tokens = (self.tokens + elapsed_s * self.refill_rate).min(self.max_tokens);
        self.last_refill_ns = now;
    }

    pub fn tokens_available(&self) -> f64 {
        self.tokens
    }

    pub fn percent_remaining(&self) -> f64 {
        if self.max_tokens > 0.0 {
            (self.tokens / self.max_tokens) * 100.0
        } else {
            0.0
        }
    }

    pub fn is_near_limit(&self, threshold_percent: f64) -> bool {
        self.percent_remaining() < threshold_percent
    }
}

pub struct RateLimiter {
    global: TokenBucket,
    per_symbol: HashMap<String, TokenBucket>,
    default_per_symbol_rate: f64,
}

impl RateLimiter {
    pub fn new(global_rate: f64, per_symbol_rate: f64) -> Self {
        Self {
            global: TokenBucket::new(global_rate, global_rate),
            per_symbol: HashMap::new(),
            default_per_symbol_rate: per_symbol_rate,
        }
    }

    #[tracing::instrument(skip(self), fields(symbol = %symbol, count = count))]
    pub fn try_consume(&mut self, symbol: &str, count: f64) -> bool {
        if !self.global.try_consume(count) {
            tracing::debug!(symbol = %symbol, "Global rate limit exceeded");
            return false;
        }

        let bucket = self.per_symbol
            .entry(symbol.to_string())
            .or_insert_with(|| TokenBucket::new(self.default_per_symbol_rate, self.default_per_symbol_rate));

        if !bucket.try_consume(count) {
            self.global.tokens += count;
            tracing::debug!(symbol = %symbol, "Per-symbol rate limit exceeded");
            return false;
        }

        true
    }

    pub fn set_symbol_rate(&mut self, symbol: &str, rate: f64) {
        let bucket = self.per_symbol
            .entry(symbol.to_string())
            .or_insert_with(|| TokenBucket::new(rate, rate));
        bucket.max_tokens = rate;
        bucket.refill_rate = rate;
    }

    pub fn get_back_pressure_status(&self, symbol: &str, threshold_percent: f64) -> Option<(f64, f64, bool)> {
        self.per_symbol.get(symbol).map(|bucket| {
            (
                bucket.tokens,
                bucket.percent_remaining(),
                bucket.is_near_limit(threshold_percent),
            )
        })
    }

    pub fn is_near_limit(&self, symbol: &str, threshold_percent: f64) -> bool {
        self.per_symbol.get(symbol)
            .map(|b| b.is_near_limit(threshold_percent))
            .unwrap_or(false)
    }

    pub fn global_tokens_remaining(&self) -> f64 {
        self.global.tokens
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_bucket_consume() {
        let mut bucket = TokenBucket::new(10.0, 10.0);
        assert!(bucket.try_consume(5.0));
        assert!((bucket.tokens_available() - 5.0).abs() < 0.1);
    }

    #[test]
    fn test_token_bucket_exhaust() {
        let mut bucket = TokenBucket::new(5.0, 1.0);
        assert!(bucket.try_consume(5.0));
        assert!(!bucket.try_consume(1.0));
    }

    #[test]
    fn test_rate_limiter_global() {
        let mut limiter = RateLimiter::new(5.0, 10.0);
        assert!(limiter.try_consume("AAPL", 1.0));
        assert!(limiter.try_consume("MSFT", 1.0));
        assert!(limiter.try_consume("AAPL", 3.0));
        assert!(!limiter.try_consume("AAPL", 1.0));
    }

    #[test]
    fn test_rate_limiter_per_symbol() {
        let mut limiter = RateLimiter::new(100.0, 2.0);
        assert!(limiter.try_consume("AAPL", 1.0));
        assert!(limiter.try_consume("AAPL", 1.0));
        assert!(limiter.try_consume("MSFT", 1.0));
    }

    #[test]
    fn test_rate_limiter_set_symbol_rate() {
        let mut limiter = RateLimiter::new(100.0, 5.0);
        limiter.set_symbol_rate("AAPL", 1.0);
        assert!(limiter.try_consume("AAPL", 1.0));
        assert!(!limiter.try_consume("AAPL", 1.0));
        assert!(limiter.try_consume("MSFT", 1.0));
    }

    #[test]
    fn test_token_bucket_percent_remaining() {
        let mut bucket = TokenBucket::new(10.0, 10.0);
        bucket.try_consume(5.0);
        let pct = bucket.percent_remaining();
        assert!((pct - 50.0).abs() < 1.0);
    }

    #[test]
    fn test_token_bucket_is_near_limit() {
        let mut bucket = TokenBucket::new(10.0, 10.0);
        bucket.try_consume(9.0);
        assert!(bucket.is_near_limit(20.0));
    }
}
