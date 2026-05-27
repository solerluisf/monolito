use crate::feature_engine::{FeatureVector, FeatureSnapshot, RegimeLabel, FeatureIndex, FEATURE_COUNT};
use crate::window_manager::WindowManager;
use market_data::NormalizedTick;

pub struct PriceComputer;
pub struct MicrostructureComputer;
pub struct MomentumComputer;
pub struct VolatilityComputer;
pub struct VolumeComputer;
pub struct RegimeComputer;

impl PriceComputer {
    pub fn compute(wm: &WindowManager, tick: &NormalizedTick) -> (f32, f32, f32) {
        let mid = tick.mid_price as f32;
        let spread_bps = tick.spread_bps as f32;
        let spread_abs = tick.spread as f32;
        (mid, spread_bps, spread_abs)
    }
}

impl MomentumComputer {
    pub fn compute(wm: &mut WindowManager) -> (f32, f32, f32, f32) {
        let rsi = wm.rsi_14.update(wm.last_mid_price) as f32;
        let ema_9 = wm.ema_9.value as f32;
        let ema_21 = wm.ema_21.value as f32;

        let macd_line = ema_9 - ema_21;
        // Proper EMA-based signal line instead of hardcoded factor
        let macd_signal = wm.macd_signal_ema.value as f32;
        let macd_histogram = macd_line - macd_signal;

        (rsi, macd_line, macd_signal, macd_histogram)
    }
}

impl VolatilityComputer {
    pub fn compute(wm: &mut WindowManager) -> (f32, f32) {
        let atr = wm.atr_14.update(
            wm.last_mid_price + wm.spread_window.last().unwrap_or(&0.01) / 2.0,
            wm.last_mid_price - wm.spread_window.last().unwrap_or(&0.01) / 2.0,
            wm.last_mid_price,
        ) as f32;
        let std_dev = wm.price_window.std_dev() as f32;
        (atr, std_dev)
    }
}

impl VolumeComputer {
    pub fn compute(wm: &WindowManager, tick: &NormalizedTick) -> f32 {
        let avg_volume = wm.volume_window.mean();
        if avg_volume > 0.0 {
            (tick.volume as f64 / avg_volume) as f32
        } else {
            1.0
        }
    }
}

impl RegimeComputer {
    pub fn compute(
        wm: &WindowManager,
        atr: f32,
        _std_dev: f32,
        macd_histogram: f32,
        volatile_atr_threshold: f64,
        strength_atr_divisor: f64,
        trending_threshold: f64,
    ) -> (RegimeLabel, f32) {
        let atr_ratio = if wm.last_mid_price > 0.0 {
            atr as f64 / wm.last_mid_price
        } else {
            0.0
        };

        let trend_strength = macd_histogram.abs();

        if atr_ratio > volatile_atr_threshold {
            (RegimeLabel::Volatile, (atr_ratio / strength_atr_divisor).min(1.0) as f32)
        } else if trend_strength > trending_threshold as f32 {
            (RegimeLabel::Trending, trend_strength.min(1.0))
        } else {
            (RegimeLabel::Ranging, (1.0 - trend_strength).min(1.0))
        }
    }
}

impl MicrostructureComputer {
    pub fn compute_order_flow_imbalance(tick: &NormalizedTick) -> f32 {
        let total = tick.bid_size + tick.ask_size;
        if total == 0 {
            0.0
        } else {
            ((tick.bid_size as f64 - tick.ask_size as f64) / total as f64) as f32
        }
    }
}

pub struct FeatureEngine {
    pub symbol: String,
    pub window_manager: WindowManager,
    pub feature_capacity: usize,
    pub volume_ratio_clamp: f64,
    pub regime_volatile_atr_threshold: f64,
    pub regime_strength_atr_divisor: f64,
    pub regime_trending_threshold: f64,
}

impl FeatureEngine {
    pub fn new(
        symbol: &str,
        rsi_period: usize,
        atr_period: usize,
        macd_signal_period: usize,
        feature_capacity: usize,
        price_window_size: usize,
        volume_window_size: usize,
        spread_window_size: usize,
        return_1_window: usize,
        return_5_window: usize,
        return_20_window: usize,
        volume_ratio_clamp: f64,
        regime_volatile_atr_threshold: f64,
        regime_strength_atr_divisor: f64,
        regime_trending_threshold: f64,
    ) -> Self {
        Self {
            symbol: symbol.to_string(),
            window_manager: WindowManager::new(
                symbol, rsi_period, atr_period, macd_signal_period,
                price_window_size, volume_window_size, spread_window_size,
                return_1_window, return_5_window, return_20_window,
            ),
            feature_capacity,
            volume_ratio_clamp,
            regime_volatile_atr_threshold,
            regime_strength_atr_divisor,
            regime_trending_threshold,
        }
    }

    #[tracing::instrument(skip_all, fields(symbol_id = %tick.symbol_id, ts = tick.timestamp_ns))]
    pub fn compute(&mut self, tick: &NormalizedTick) -> FeatureVector {
        self.window_manager.update(
            tick.mid_price,
            tick.volume as f64,
            tick.spread,
        );

        let mut fv = FeatureVector::new(tick.symbol_id, tick.timestamp_ns);

        let (mid, spread_bps, spread_abs) = PriceComputer::compute(&self.window_manager, tick);
        // Clamp spread_bps to a sane max (500 bps = 5%) to avoid poisoning from corrupt data
        let spread_bps = spread_bps.min(500.0).max(0.0);
        fv.set(FeatureIndex::MidPrice, mid);
        fv.set(FeatureIndex::SpreadBps, spread_bps);
        fv.set(FeatureIndex::SpreadAbs, spread_abs);

        let (rsi, macd_line, macd_signal, macd_histogram) =
            MomentumComputer::compute(&mut self.window_manager);
        fv.set(FeatureIndex::Rsi14, rsi);
        fv.set(FeatureIndex::MacdLine, macd_line);
        fv.set(FeatureIndex::MacdSignal, macd_signal);
        fv.set(FeatureIndex::MacdHistogram, macd_histogram);

        let (atr, std_dev) = VolatilityComputer::compute(&mut self.window_manager);
        fv.set(FeatureIndex::Atr14, atr);
        fv.set(FeatureIndex::RollingStd, std_dev);

        let volume_ratio = VolumeComputer::compute(&self.window_manager, tick)
            .min(self.volume_ratio_clamp as f32)
            .max(0.0);
        fv.set(FeatureIndex::VolumeRatio, volume_ratio);

        let ofi = MicrostructureComputer::compute_order_flow_imbalance(tick);
        fv.set(FeatureIndex::OrderFlowImbalance, ofi);

        let (regime, regime_strength) = RegimeComputer::compute(
            &self.window_manager,
            atr,
            std_dev,
            macd_histogram,
            self.regime_volatile_atr_threshold,
            self.regime_strength_atr_divisor,
            self.regime_trending_threshold,
        );
        fv.set(FeatureIndex::Regime, regime as i32 as f32);
        fv.set(FeatureIndex::RegimeStrength, regime_strength);

        fv.set(FeatureIndex::Ema9, self.window_manager.ema_9.value as f32);
        fv.set(FeatureIndex::Ema21, self.window_manager.ema_21.value as f32);
        fv.set(FeatureIndex::Ema50, self.window_manager.ema_50.value as f32);

        fv
    }

    pub fn compute_snapshot(&mut self, tick: &NormalizedTick) -> FeatureSnapshot {
        let fv = self.compute(tick);

        FeatureSnapshot {
            symbol_id: tick.symbol_id,
            timestamp_ns: tick.timestamp_ns,
            mid_price: fv.get(FeatureIndex::MidPrice),
            spread_bps: fv.get(FeatureIndex::SpreadBps),
            rsi_14: fv.get(FeatureIndex::Rsi14),
            macd_line: fv.get(FeatureIndex::MacdLine),
            macd_signal: fv.get(FeatureIndex::MacdSignal),
            macd_histogram: fv.get(FeatureIndex::MacdHistogram),
            atr_14: fv.get(FeatureIndex::Atr14),
            ema_9: fv.get(FeatureIndex::Ema9),
            ema_21: fv.get(FeatureIndex::Ema21),
            ema_50: fv.get(FeatureIndex::Ema50),
            volume_ratio: fv.get(FeatureIndex::VolumeRatio),
            order_flow_imbalance: fv.get(FeatureIndex::OrderFlowImbalance),
            regime: match fv.get(FeatureIndex::Regime) as i32 {
                0 => RegimeLabel::Ranging,
                1 => RegimeLabel::Trending,
                _ => RegimeLabel::Volatile,
            },
            regime_strength: fv.get(FeatureIndex::RegimeStrength),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use market_data::NormalizedTick;
    use unified_trading_core::symbol_registry::SymbolId;

    fn make_tick(symbol_id: SymbolId, ts: u64, mid: f64, spread: f64) -> NormalizedTick {
        NormalizedTick {
            symbol_id,
            timestamp_ns: ts,
            mid_price: mid,
            spread,
            spread_bps: (spread / mid) * 10_000.0,
            bid: mid - spread / 2.0,
            ask: mid + spread / 2.0,
            bid_size: 100,
            ask_size: 200,
            volume: 50,
        }
    }

    #[test]
    fn test_feature_engine_compute() {
        use unified_trading_core::symbol_registry::SymbolId;
        let mut engine = FeatureEngine::new("AAPL", 14, 14, 9, 20, 50, 20, 20, 1, 5, 20, 0.3, 0.02, 0.05, 0.5);
        let tick = make_tick(SymbolId::from_raw(0), 1000, 150.0, 0.05);
        let fv = engine.compute(&tick);
        assert_eq!(fv.len(), FEATURE_COUNT);
        assert!(fv.get(FeatureIndex::MidPrice) > 0.0);
        assert!(fv.get(FeatureIndex::Rsi14) > 0.0);
    }

    #[test]
    fn test_feature_engine_snapshot() {
        use unified_trading_core::symbol_registry::SymbolId;
        let mut engine = FeatureEngine::new("MSFT", 14, 14, 9, 20, 50, 20, 20, 1, 5, 20, 0.3, 0.02, 0.05, 0.5);
        let tick = make_tick(SymbolId::from_raw(1), 2000, 400.0, 0.04);
        let snap = engine.compute_snapshot(&tick);
        assert_eq!(snap.symbol_id, SymbolId::from_raw(1));
        assert!((snap.mid_price - 400.0).abs() < 0.01);
    }

    #[test]
    fn test_feature_engine_multiple_ticks() {
        let mut engine = FeatureEngine::new("AAPL", 14, 14, 9, 20, 50, 20, 20, 1, 5, 20, 0.3, 0.02, 0.05, 0.5);
        for i in 0..20 {
            let tick = make_tick(SymbolId::from_raw(0), i * 1000, 150.0 + (i as f64 * 0.01), 0.05);
            engine.compute(&tick);
        }
        let tick = make_tick(SymbolId::from_raw(0), 20000, 150.2, 0.05);
        let fv = engine.compute(&tick);
        assert!(fv.get(FeatureIndex::Ema9) > 0.0);
        assert!(fv.get(FeatureIndex::Atr14) > 0.0);
    }

    #[test]
    fn test_price_computer() {
        let tick = make_tick(SymbolId::from_raw(0), 1000, 150.0, 0.05);
        let wm = WindowManager::new("AAPL", 14, 14, 9, 50, 20, 20, 1, 5, 20);
        let (mid, spread_bps, spread_abs) = PriceComputer::compute(&wm, &tick);
        assert!((mid - 150.0).abs() < 0.001);
        assert!((spread_bps - 3.33).abs() < 0.1);
        assert!((spread_abs - 0.05).abs() < 0.001);
    }

    #[test]
    fn test_volume_computer() {
        let tick = make_tick(SymbolId::from_raw(0), 1000, 150.0, 0.05);
        let mut wm = WindowManager::new("AAPL", 14, 14, 9, 50, 20, 20, 1, 5, 20);
        wm.volume_window.push(1000.0);
        let ratio = VolumeComputer::compute(&wm, &tick);
        assert!((ratio - 0.05).abs() < 0.001);
    }

    #[test]
    fn test_regime_computer_volatile() {
        let mut wm = WindowManager::new("AAPL", 14, 14, 9, 50, 20, 20, 1, 5, 20);
        wm.last_mid_price = 100.0;
        let (regime, strength) = RegimeComputer::compute(&wm, 3.0, 1.0, 0.1, 0.02, 0.05, 0.5);
        assert!(matches!(regime, RegimeLabel::Volatile));
        assert!(strength > 0.0);
    }

    #[test]
    fn test_regime_computer_ranging() {
        let mut wm = WindowManager::new("AAPL", 14, 14, 9, 50, 20, 20, 1, 5, 20);
        wm.last_mid_price = 100.0;
        let (regime, strength) = RegimeComputer::compute(&wm, 0.001, 0.001, 0.01, 0.02, 0.05, 0.5);
        assert!(matches!(regime, RegimeLabel::Ranging));
    }
}
