pub struct ReplayController {
    recorded_ticks: Vec<crate::normalizer::RawTick>,
    current_index: usize,
    is_replaying: bool,
}

impl ReplayController {
    pub fn new() -> Self {
        Self {
            recorded_ticks: Vec::new(),
            current_index: 0,
            is_replaying: false,
        }
    }

    pub fn load_ticks(&mut self, ticks: Vec<crate::normalizer::RawTick>) {
        self.recorded_ticks = ticks;
        self.current_index = 0;
    }

    pub fn start_replay(&mut self) {
        self.is_replaying = true;
        self.current_index = 0;
    }

    pub fn stop_replay(&mut self) {
        self.is_replaying = false;
    }

    pub fn next_tick(&mut self) -> Option<crate::normalizer::RawTick> {
        if !self.is_replaying || self.current_index >= self.recorded_ticks.len() {
            return None;
        }
        let tick = self.recorded_ticks[self.current_index].clone();
        self.current_index += 1;
        Some(tick)
    }

    pub fn is_replaying(&self) -> bool {
        self.is_replaying
    }

    pub fn progress(&self) -> (usize, usize) {
        (self.current_index, self.recorded_ticks.len())
    }
}

impl Default for ReplayController {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::normalizer::RawTick;

    fn make_tick(ts: u64) -> RawTick {
        RawTick {
            symbol: "AAPL".to_string(),
            timestamp_ns: ts,
            bid: 150.0,
            ask: 150.05,
            bid_size: 100,
            ask_size: 200,
            last_price: 150.02,
            last_size: 50,
            exchange: "IEX".to_string(),
        }
    }

    #[test]
    fn test_replay_controller_basic() {
        let mut rc = ReplayController::new();
        rc.load_ticks(vec![make_tick(1), make_tick(2), make_tick(3)]);
        rc.start_replay();

        assert_eq!(rc.next_tick().unwrap().timestamp_ns, 1);
        assert_eq!(rc.next_tick().unwrap().timestamp_ns, 2);
        assert_eq!(rc.next_tick().unwrap().timestamp_ns, 3);
        assert!(rc.next_tick().is_none());
    }

    #[test]
    fn test_replay_controller_not_replaying() {
        let mut rc = ReplayController::new();
        rc.load_ticks(vec![make_tick(1)]);
        assert!(rc.next_tick().is_none());
    }

    #[test]
    fn test_replay_controller_progress() {
        let mut rc = ReplayController::new();
        rc.load_ticks(vec![make_tick(1), make_tick(2)]);
        rc.start_replay();
        rc.next_tick();
        assert_eq!(rc.progress(), (1, 2));
    }

    #[test]
    fn test_replay_controller_stop() {
        let mut rc = ReplayController::new();
        rc.load_ticks(vec![make_tick(1), make_tick(2)]);
        rc.start_replay();
        rc.next_tick();
        rc.stop_replay();
        assert!(rc.next_tick().is_none());
    }
}
