use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureVector {
    pub symbol: String,
    pub timestamp_ns: u64,
    pub values: Vec<f32>,
    pub feature_names: Vec<String>,
}

impl FeatureVector {
    pub fn new(symbol: &str, timestamp_ns: u64, capacity: usize) -> Self {
        Self {
            symbol: symbol.to_string(),
            timestamp_ns,
            values: Vec::with_capacity(capacity),
            feature_names: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, name: &str, value: f32) {
        self.feature_names.push(name.to_string());
        self.values.push(value);
    }

    pub fn get(&self, name: &str) -> Option<f32> {
        self.feature_names
            .iter()
            .position(|n| n == name)
            .map(|i| self.values[i])
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn to_array<const N: usize>(&self) -> [f32; N] {
        let mut arr = [0.0f32; N];
        let len = self.values.len().min(N);
        arr[..len].copy_from_slice(&self.values[..len]);
        arr
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureSnapshot {
    pub symbol: String,
    pub timestamp_ns: u64,
    pub mid_price: f32,
    pub spread_bps: f32,
    pub rsi_14: f32,
    pub macd_line: f32,
    pub macd_signal: f32,
    pub macd_histogram: f32,
    pub atr_14: f32,
    pub ema_9: f32,
    pub ema_21: f32,
    pub ema_50: f32,
    pub volume_ratio: f32,
    pub order_flow_imbalance: f32,
    pub regime: RegimeLabel,
    pub regime_strength: f32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum RegimeLabel {
    Ranging,
    Trending,
    Volatile,
}

impl Default for RegimeLabel {
    fn default() -> Self {
        Self::Ranging
    }
}

pub struct RollingWindow<T> {
    data: Vec<T>,
    capacity: usize,
    index: usize,
    full: bool,
}

impl<T: Clone + Default> RollingWindow<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            data: vec![T::default(); capacity],
            capacity,
            index: 0,
            full: false,
        }
    }

    pub fn push(&mut self, value: T) {
        self.data[self.index] = value;
        self.index = (self.index + 1) % self.capacity;
        if self.index == 0 {
            self.full = true;
        }
    }

    pub fn values(&self) -> Vec<T> {
        if self.full {
            let mut result = Vec::with_capacity(self.capacity);
            result.extend_from_slice(&self.data[self.index..]);
            result.extend_from_slice(&self.data[..self.index]);
            result
        } else {
            self.data[..self.index].to_vec()
        }
    }

    pub fn len(&self) -> usize {
        if self.full {
            self.capacity
        } else {
            self.index
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn last(&self) -> Option<&T> {
        if self.is_empty() {
            None
        } else if self.index == 0 {
            Some(&self.data[self.capacity - 1])
        } else {
            Some(&self.data[self.index - 1])
        }
    }

    pub fn mean(&self) -> f64
    where
        T: Into<f64> + Copy,
    {
        let vals = self.values();
        if vals.is_empty() {
            return 0.0;
        }
        vals.iter().map(|v| Into::<f64>::into(*v)).sum::<f64>() / vals.len() as f64
    }

    pub fn std_dev(&self) -> f64
    where
        T: Into<f64> + Copy,
    {
        let vals = self.values();
        if vals.len() < 2 {
            return 0.0;
        }
        let mean = self.mean();
        let variance: f64 = vals
            .iter()
            .map(|v| {
                let d = Into::<f64>::into(*v) - mean;
                d * d
            })
            .sum::<f64>()
            / (vals.len() - 1) as f64;
        variance.sqrt()
    }
}

pub struct EMAState {
    pub value: f64,
    pub multiplier: f64,
    pub initialized: bool,
}

impl EMAState {
    pub fn new(period: usize) -> Self {
        Self {
            value: 0.0,
            multiplier: 2.0 / (period + 1) as f64,
            initialized: false,
        }
    }

    pub fn update(&mut self, price: f64) -> f64 {
        if !self.initialized {
            self.value = price;
            self.initialized = true;
        } else {
            self.value = (price - self.value) * self.multiplier + self.value;
        }
        self.value
    }
}

pub struct RSIState {
    pub gains: RollingWindow<f64>,
    pub losses: RollingWindow<f64>,
    pub last_price: f64,
}

impl RSIState {
    pub fn new(period: usize) -> Self {
        Self {
            gains: RollingWindow::new(period),
            losses: RollingWindow::new(period),
            last_price: 0.0,
        }
    }

    pub fn update(&mut self, price: f64) -> f64 {
        if self.last_price > 0.0 {
            let change = price - self.last_price;
            if change > 0.0 {
                self.gains.push(change);
                self.losses.push(0.0);
            } else {
                self.gains.push(0.0);
                self.losses.push(-change);
            }
        }
        self.last_price = price;

        let avg_gain = self.gains.mean();
        let avg_loss = self.losses.mean();

        if avg_loss == 0.0 {
            100.0
        } else {
            let rs = avg_gain / avg_loss;
            100.0 - (100.0 / (1.0 + rs))
        }
    }
}

pub struct ATRState {
    pub tr_values: RollingWindow<f64>,
    pub last_high: f64,
    pub last_low: f64,
    pub last_close: f64,
}

impl ATRState {
    pub fn new(period: usize) -> Self {
        Self {
            tr_values: RollingWindow::new(period),
            last_high: 0.0,
            last_low: 0.0,
            last_close: 0.0,
        }
    }

    pub fn update(&mut self, high: f64, low: f64, close: f64) -> f64 {
        if self.last_close > 0.0 {
            let tr = (high - low)
                .max((high - self.last_close).abs())
                .max((low - self.last_close).abs());
            self.tr_values.push(tr);
        }
        self.last_high = high;
        self.last_low = low;
        self.last_close = close;
        self.tr_values.mean()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feature_vector() {
        let mut fv = FeatureVector::new("AAPL", 1000, 10);
        fv.push("mid_price", 150.0);
        fv.push("rsi", 65.0);
        assert_eq!(fv.len(), 2);
        assert_eq!(fv.get("mid_price"), Some(150.0));
        assert_eq!(fv.get("missing"), None);
    }

    #[test]
    fn test_rolling_window() {
        let mut rw = RollingWindow::new(5);
        for i in 1..=7 {
            rw.push(i as f64);
        }
        assert_eq!(rw.len(), 5);
        let vals = rw.values();
        assert_eq!(vals.len(), 5);
    }

    #[test]
    fn test_rolling_window_mean() {
        let mut rw = RollingWindow::new(4);
        rw.push(2.0f64);
        rw.push(4.0);
        rw.push(4.0);
        rw.push(4.0);
        assert!((rw.mean() - 3.5).abs() < 0.001);
    }

    #[test]
    fn test_ema_state() {
        let mut ema = EMAState::new(9);
        ema.update(100.0);
        assert!(ema.initialized);
        assert_eq!(ema.value, 100.0);
        ema.update(101.0);
        assert!(ema.value > 100.0 && ema.value < 101.0);
    }

    #[test]
    fn test_rsi_state() {
        let mut rsi = RSIState::new(14);
        rsi.last_price = 100.0;
        for _ in 0..14 {
            rsi.update(101.0);
        }
        let val = rsi.update(102.0);
        assert!(val >= 0.0 && val <= 100.0);
    }

    #[test]
    fn test_atr_state() {
        let mut atr = ATRState::new(14);
        atr.last_close = 100.0;
        atr.update(101.0, 99.0, 100.5);
        let val = atr.update(102.0, 100.0, 101.0);
        assert!(val > 0.0);
    }

    #[test]
    fn test_feature_vector_to_array() {
        let mut fv = FeatureVector::new("AAPL", 1000, 10);
        fv.push("a", 1.0);
        fv.push("b", 2.0);
        let arr: [f32; 5] = fv.to_array();
        assert_eq!(arr[0], 1.0);
        assert_eq!(arr[1], 2.0);
        assert_eq!(arr[2], 0.0);
    }
}
