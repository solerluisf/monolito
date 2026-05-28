use serde::{Deserialize, Serialize};
use unified_trading_core::symbol_registry::SymbolId;

/// Current feature schema version. Increment this when the feature set changes
/// to prevent training/serving skew between model and inference engine.
pub const FEATURE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FeatureIndex {
    MidPrice = 0,
    SpreadBps = 1,
    SpreadAbs = 2,
    Rsi14 = 3,
    MacdLine = 4,
    MacdSignal = 5,
    MacdHistogram = 6,
    Atr14 = 7,
    RollingStd = 8,
    VolumeRatio = 9,
    OrderFlowImbalance = 10,
    Regime = 11,
    RegimeStrength = 12,
    Ema9 = 13,
    Ema21 = 14,
    Ema50 = 15,
    Confidence = 16,
}

pub const FEATURE_COUNT: usize = 17;

static FEATURE_NAMES: [&str; FEATURE_COUNT] = [
    "mid_price",
    "spread_bps",
    "spread_abs",
    "rsi_14",
    "macd_line",
    "macd_signal",
    "macd_histogram",
    "atr_14",
    "rolling_std",
    "volume_ratio",
    "order_flow_imbalance",
    "regime",
    "regime_strength",
    "ema_9",
    "ema_21",
    "ema_50",
    "confidence",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureVector {
    pub symbol_id: SymbolId,
    pub timestamp_ns: u64,
    pub values: [f32; FEATURE_COUNT],
    /// Schema version for training/serving skew detection
    pub feature_schema_version: u32,
}

impl FeatureVector {
    pub fn new(symbol_id: SymbolId, timestamp_ns: u64) -> Self {
        Self {
            symbol_id,
            timestamp_ns,
            values: [0.0f32; FEATURE_COUNT],
            feature_schema_version: FEATURE_SCHEMA_VERSION,
        }
    }

    pub fn set(&mut self, index: FeatureIndex, value: f32) {
        self.values[index as usize] = value;
    }

    pub fn get(&self, index: FeatureIndex) -> f32 {
        self.values[index as usize]
    }

    pub fn len(&self) -> usize {
        FEATURE_COUNT
    }

    pub fn is_empty(&self) -> bool {
        false
    }

    pub fn to_array<const N: usize>(&self) -> [f32; N] {
        let mut arr = [0.0f32; N];
        let len = self.values.len().min(N);
        arr[..len].copy_from_slice(&self.values[..len]);
        arr
    }

    pub fn feature_name(index: FeatureIndex) -> &'static str {
        FEATURE_NAMES[index as usize]
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureSnapshot {
    pub symbol_id: SymbolId,
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
        let sum = vals.iter().map(|v| Into::<f64>::into(*v)).sum::<f64>();
        if !sum.is_finite() {
            return 0.0;
        }
        sum / vals.len() as f64
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
        if !variance.is_finite() || variance < 0.0 {
            return 0.0;
        }
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
mod proptest_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn rolling_window_mean_is_finite(values in prop::collection::vec(any::<f64>(), 0..100)) {
            let mut rw = RollingWindow::new(20);
            for v in values {
                if v.is_finite() {
                    rw.push(v);
                }
            }
            let mean = rw.mean();
            prop_assert!(mean.is_finite(), "mean should be finite, got {}", mean);
        }

        #[test]
        fn rolling_window_std_dev_non_negative(values in prop::collection::vec(any::<f64>(), 0..100)) {
            let mut rw = RollingWindow::new(20);
            for v in values {
                if v.is_finite() {
                    rw.push(v);
                }
            }
            let std_dev = rw.std_dev();
            prop_assert!(std_dev >= 0.0, "std_dev should be non-negative, got {}", std_dev);
            prop_assert!(std_dev.is_finite() || std_dev == 0.0,
                "std_dev should be finite, got {}", std_dev);
        }

        #[test]
        fn rsi_state_never_nan_or_inf(prices in prop::collection::vec(any::<f64>(), 0..50)) {
            let mut rsi = RSIState::new(14);
            rsi.last_price = 100.0;
            for price in prices {
                if price > 0.0 && price.is_finite() {
                    let val = rsi.update(price);
                    prop_assert!(val.is_finite(), "RSI should be finite, got {}", val);
                    prop_assert!(val >= 0.0 && val <= 100.0,
                        "RSI should be in [0,100], got {}", val);
                }
            }
        }

        #[test]
        fn atr_state_never_nan_or_inf(highs in prop::collection::vec(any::<f64>(), 0..50),
                                       lows in prop::collection::vec(any::<f64>(), 0..50),
                                       closes in prop::collection::vec(any::<f64>(), 0..50)) {
            let mut atr = ATRState::new(14);
            atr.last_close = 100.0;
            for i in 0..highs.len().min(lows.len()).min(closes.len()) {
                let h = highs[i];
                let l = lows[i];
                let c = closes[i];
                if h > 0.0 && h.is_finite() && l > 0.0 && l.is_finite() && c > 0.0 && c.is_finite() && h >= l {
                    let val = atr.update(h, l, c);
                    prop_assert!(val.is_finite() && val >= 0.0,
                        "ATR should be finite and non-negative, got {}", val);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use unified_trading_core::symbol_registry::SymbolId;

    #[test]
    fn test_feature_vector() {
        let symbol_id = SymbolId::from_raw(0);
        let mut fv = FeatureVector::new(symbol_id, 1000);
        fv.set(FeatureIndex::MidPrice, 150.0);
        fv.set(FeatureIndex::Rsi14, 65.0);
        assert_eq!(fv.len(), FEATURE_COUNT);
        assert_eq!(fv.get(FeatureIndex::MidPrice), 150.0);
        assert_eq!(fv.get(FeatureIndex::Confidence), 0.0);
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
        let symbol_id = SymbolId::from_raw(0);
        let mut fv = FeatureVector::new(symbol_id, 1000);
        fv.set(FeatureIndex::MidPrice, 1.0);
        fv.set(FeatureIndex::Rsi14, 2.0);
        let arr: [f32; 5] = fv.to_array();
        assert_eq!(arr[0], 1.0);
        assert_eq!(arr[1], 0.0);
        assert_eq!(arr[2], 0.0);
    }
}
