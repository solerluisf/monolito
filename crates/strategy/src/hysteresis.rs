pub struct HysteresisFilter {
    pub deadband: f64,
    state: i32,
}

impl HysteresisFilter {
    pub fn new(deadband: f64) -> Self {
        Self {
            deadband,
            state: 0,
        }
    }

    pub fn evaluate(&mut self, score: f64, long_threshold: f64, short_threshold: f64) -> i32 {
        match self.state {
            0 => {
                if score > long_threshold {
                    self.state = 1;
                    1
                } else if score < short_threshold {
                    self.state = -1;
                    -1
                } else {
                    0
                }
            }
            1 => {
                if score < long_threshold - self.deadband {
                    self.state = 0;
                    0
                } else {
                    1
                }
            }
            -1 => {
                if score > short_threshold + self.deadband {
                    self.state = 0;
                    0
                } else {
                    -1
                }
            }
            _ => 0,
        }
    }

    pub fn reset(&mut self) {
        self.state = 0;
    }

    pub fn current_state(&self) -> i32 {
        self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hysteresis_initial_state() {
        let h = HysteresisFilter::new(0.15);
        assert_eq!(h.current_state(), 0);
    }

    #[test]
    fn test_hysteresis_long_entry() {
        let mut h = HysteresisFilter::new(0.15);
        assert_eq!(h.evaluate(0.8, 0.6, -0.6), 1);
        assert_eq!(h.current_state(), 1);
    }

    #[test]
    fn test_hysteresis_prevents_flip() {
        let mut h = HysteresisFilter::new(0.15);
        h.evaluate(0.8, 0.6, -0.6);
        assert_eq!(h.evaluate(0.5, 0.6, -0.6), 1);
    }

    #[test]
    fn test_hysteresis_exit() {
        let mut h = HysteresisFilter::new(0.15);
        h.evaluate(0.8, 0.6, -0.6);
        assert_eq!(h.evaluate(0.4, 0.6, -0.6), 0);
    }

    #[test]
    fn test_hysteresis_short_entry() {
        let mut h = HysteresisFilter::new(0.15);
        assert_eq!(h.evaluate(-0.8, 0.6, -0.6), -1);
    }

    #[test]
    fn test_hysteresis_reset() {
        let mut h = HysteresisFilter::new(0.15);
        h.evaluate(0.8, 0.6, -0.6);
        h.reset();
        assert_eq!(h.current_state(), 0);
    }
}
