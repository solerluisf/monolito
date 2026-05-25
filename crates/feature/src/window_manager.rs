use crate::feature_engine::{EMAState, RSIState, ATRState, RollingWindow};

pub struct WindowManager {
    pub symbol: String,
    pub price_window: RollingWindow<f64>,
    pub volume_window: RollingWindow<f64>,
    pub spread_window: RollingWindow<f64>,
    pub ema_9: EMAState,
    pub ema_21: EMAState,
    pub ema_50: EMAState,
    pub macd_signal_ema: EMAState,
    pub rsi_14: RSIState,
    pub atr_14: ATRState,
    pub return_1: RollingWindow<f64>,
    pub return_5: RollingWindow<f64>,
    pub return_20: RollingWindow<f64>,
    pub last_mid_price: f64,
}

impl WindowManager {
    pub fn new(
        symbol: &str,
        rsi_period: usize,
        atr_period: usize,
        macd_signal_period: usize,
        price_window_size: usize,
        volume_window_size: usize,
        spread_window_size: usize,
        return_1_window: usize,
        return_5_window: usize,
        return_20_window: usize,
    ) -> Self {
        Self {
            symbol: symbol.to_string(),
            price_window: RollingWindow::new(price_window_size),
            volume_window: RollingWindow::new(volume_window_size),
            spread_window: RollingWindow::new(spread_window_size),
            ema_9: EMAState::new(9),
            ema_21: EMAState::new(21),
            ema_50: EMAState::new(50),
            macd_signal_ema: EMAState::new(macd_signal_period),
            rsi_14: RSIState::new(rsi_period),
            atr_14: ATRState::new(atr_period),
            return_1: RollingWindow::new(return_1_window),
            return_5: RollingWindow::new(return_5_window),
            return_20: RollingWindow::new(return_20_window),
            last_mid_price: 0.0,
        }
    }

    pub fn update(&mut self, mid_price: f64, volume: f64, spread: f64) {
        self.price_window.push(mid_price);
        self.volume_window.push(volume);
        self.spread_window.push(spread);

        self.ema_9.update(mid_price);
        self.ema_21.update(mid_price);
        self.ema_50.update(mid_price);

        let macd_line = self.ema_9.value - self.ema_21.value;
        self.macd_signal_ema.update(macd_line);

        self.rsi_14.update(mid_price);

        let high = mid_price + spread / 2.0;
        let low = mid_price - spread / 2.0;
        self.atr_14.update(high, low, mid_price);

        if self.last_mid_price > 0.0 {
            let ret = (mid_price - self.last_mid_price) / self.last_mid_price;
            self.return_1.push(ret);
            self.return_5.push(ret);
            self.return_20.push(ret);
        }
        self.last_mid_price = mid_price;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_manager_update() {
        let mut wm = WindowManager::new("AAPL", 14, 14, 9, 50, 20, 20, 1, 5, 20);
        wm.update(150.0, 1000.0, 0.05);
        wm.update(150.1, 1200.0, 0.04);
        wm.update(150.2, 800.0, 0.06);

        assert!((wm.last_mid_price - 150.2).abs() < 0.001);
        assert!(wm.ema_9.initialized);
        assert!(wm.rsi_14.last_price > 0.0);
    }

    #[test]
    fn test_window_manager_returns() {
        let mut wm = WindowManager::new("MSFT", 14, 14, 9, 50, 20, 20, 1, 5, 20);
        wm.last_mid_price = 400.0;
        wm.update(401.0, 500.0, 0.04);
        assert!(wm.return_1.len() == 1);
    }
}
