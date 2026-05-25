use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OrderState {
    Pending,
    Submitted,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
    Expired,
    Replaced,
}

impl OrderState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            OrderState::Filled
                | OrderState::Cancelled
                | OrderState::Rejected
                | OrderState::Expired
                | OrderState::Replaced
        )
    }

    pub fn is_active(&self) -> bool {
        matches!(
            self,
            OrderState::Pending
                | OrderState::Submitted
                | OrderState::PartiallyFilled
        )
    }

    pub fn allowed_transitions(&self) -> &'static [OrderState] {
        match self {
            OrderState::Pending => &[OrderState::Submitted, OrderState::Rejected],
            OrderState::Submitted => &[
                OrderState::PartiallyFilled,
                OrderState::Filled,
                OrderState::Cancelled,
                OrderState::Rejected,
            ],
            OrderState::PartiallyFilled => &[
                OrderState::PartiallyFilled,
                OrderState::Filled,
                OrderState::Cancelled,
            ],
            OrderState::Filled => &[],
            OrderState::Cancelled => &[],
            OrderState::Rejected => &[],
            OrderState::Expired => &[],
            OrderState::Replaced => &[],
        }
    }

    pub fn can_transition_to(&self, next: &OrderState) -> bool {
        self.allowed_transitions().contains(next)
    }
}

#[derive(Debug, Clone)]
pub enum OrderEvent {
    Submit,
    PartialFill { filled_qty: f64, fill_price: f64 },
    FullFill { filled_qty: f64, fill_price: f64 },
    Cancel,
    Reject { reason: String },
    Expire,
    Replace,
}

#[derive(Debug, Clone)]
pub struct TransitionError {
    pub from: OrderState,
    pub to: OrderState,
    pub event: OrderEvent,
    pub reason: String,
}

impl std::fmt::Display for TransitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Invalid transition: {:?} -> {:?} (event: {:?}) - {}",
            self.from, self.to, self.event, self.reason
        )
    }
}

impl std::error::Error for TransitionError {}

pub trait OrderStateBehavior: Send + Sync {
    fn state_name(&self) -> &'static str;
    fn on_enter(&self, _order_id: &str, _metadata: &mut OrderMetadata) {}
    fn on_exit(&self, _order_id: &str, _metadata: &mut OrderMetadata) {}
    fn allowed_events(&self) -> Vec<OrderEvent>;
}

#[derive(Debug, Clone, Default)]
pub struct OrderMetadata {
    pub total_filled_qty: f64,
    pub avg_fill_price: f64,
    pub last_event_timestamp_ns: u64,
    pub transition_count: u64,
    pub rejection_reason: Option<String>,
}

pub struct PendingState;
pub struct SubmittedState;
pub struct PartiallyFilledState;
pub struct TerminalState {
    name: &'static str,
}

impl OrderStateBehavior for PendingState {
    fn state_name(&self) -> &'static str { "Pending" }
    fn allowed_events(&self) -> Vec<OrderEvent> {
        vec![OrderEvent::Submit]
    }
}

impl OrderStateBehavior for SubmittedState {
    fn state_name(&self) -> &'static str { "Submitted" }
    fn allowed_events(&self) -> Vec<OrderEvent> {
        vec![
            OrderEvent::PartialFill { filled_qty: 0.0, fill_price: 0.0 },
            OrderEvent::FullFill { filled_qty: 0.0, fill_price: 0.0 },
            OrderEvent::Cancel,
        ]
    }
}

impl OrderStateBehavior for PartiallyFilledState {
    fn state_name(&self) -> &'static str { "PartiallyFilled" }
    fn allowed_events(&self) -> Vec<OrderEvent> {
        vec![
            OrderEvent::PartialFill { filled_qty: 0.0, fill_price: 0.0 },
            OrderEvent::FullFill { filled_qty: 0.0, fill_price: 0.0 },
            OrderEvent::Cancel,
        ]
    }
}

impl OrderStateBehavior for TerminalState {
    fn state_name(&self) -> &'static str { self.name }
    fn allowed_events(&self) -> Vec<OrderEvent> { vec![] }
}

pub struct OrderStateMachine {
    current_state: OrderState,
    metadata: OrderMetadata,
    state_behavior: Box<dyn OrderStateBehavior>,
    transition_log: Vec<(OrderState, OrderState, OrderEvent, u64)>,
    transition_count: AtomicU64,
}

impl OrderStateMachine {
    pub fn new() -> Self {
        Self {
            current_state: OrderState::Pending,
            metadata: OrderMetadata::default(),
            state_behavior: Box::new(PendingState),
            transition_log: Vec::new(),
            transition_count: AtomicU64::new(0),
        }
    }

    pub fn current_state(&self) -> &OrderState {
        &self.current_state
    }

    pub fn metadata(&self) -> &OrderMetadata {
        &self.metadata
    }

    pub fn transition_log(&self) -> &[(OrderState, OrderState, OrderEvent, u64)] {
        &self.transition_log
    }

    pub fn transition_count(&self) -> u64 {
        self.transition_count.load(Ordering::Relaxed)
    }

    pub fn apply_event(&mut self, event: OrderEvent, timestamp_ns: u64) -> Result<OrderState, TransitionError> {
        let target_state = self.resolve_target_state(&event)?;

        if !self.current_state.can_transition_to(&target_state) {
            return Err(TransitionError {
                from: self.current_state.clone(),
                to: target_state.clone(),
                event: event.clone(),
                reason: format!(
                    "Transition from {:?} to {:?} is not allowed. Allowed: {:?}",
                    self.current_state,
                    target_state,
                    self.current_state.allowed_transitions()
                ),
            });
        }

        let old_state = self.current_state.clone();
        let order_id = "unknown";

        self.state_behavior.on_exit(order_id, &mut self.metadata);

        self.update_metadata_for_event(&event, timestamp_ns);

        self.current_state = target_state.clone();
        self.state_behavior = self.create_behavior_for_state(&target_state);

        self.state_behavior.on_enter(order_id, &mut self.metadata);

        self.metadata.transition_count += 1;
        self.transition_count.fetch_add(1, Ordering::Relaxed);

        self.transition_log.push((
            old_state.clone(),
            target_state.clone(),
            event,
            timestamp_ns,
        ));

        Ok(target_state)
    }

    pub fn submit(&mut self, timestamp_ns: u64) -> Result<OrderState, TransitionError> {
        self.apply_event(OrderEvent::Submit, timestamp_ns)
    }

    pub fn partial_fill(&mut self, filled_qty: f64, fill_price: f64, timestamp_ns: u64) -> Result<OrderState, TransitionError> {
        self.apply_event(OrderEvent::PartialFill { filled_qty, fill_price }, timestamp_ns)
    }

    pub fn full_fill(&mut self, filled_qty: f64, fill_price: f64, timestamp_ns: u64) -> Result<OrderState, TransitionError> {
        self.apply_event(OrderEvent::FullFill { filled_qty, fill_price }, timestamp_ns)
    }

    pub fn cancel(&mut self, timestamp_ns: u64) -> Result<OrderState, TransitionError> {
        self.apply_event(OrderEvent::Cancel, timestamp_ns)
    }

    pub fn reject(&mut self, reason: String, timestamp_ns: u64) -> Result<OrderState, TransitionError> {
        self.apply_event(OrderEvent::Reject { reason }, timestamp_ns)
    }

    pub fn expire(&mut self, timestamp_ns: u64) -> Result<OrderState, TransitionError> {
        self.apply_event(OrderEvent::Expire, timestamp_ns)
    }

    pub fn replace(&mut self, timestamp_ns: u64) -> Result<OrderState, TransitionError> {
        self.apply_event(OrderEvent::Replace, timestamp_ns)
    }

    pub fn is_terminal(&self) -> bool {
        self.current_state.is_terminal()
    }

    pub fn is_active(&self) -> bool {
        self.current_state.is_active()
    }

    fn resolve_target_state(&self, event: &OrderEvent) -> Result<OrderState, TransitionError> {
        match event {
            OrderEvent::Submit => Ok(OrderState::Submitted),
            OrderEvent::PartialFill { .. } => Ok(OrderState::PartiallyFilled),
            OrderEvent::FullFill { .. } => Ok(OrderState::Filled),
            OrderEvent::Cancel => Ok(OrderState::Cancelled),
            OrderEvent::Reject { .. } => Ok(OrderState::Rejected),
            OrderEvent::Expire => Ok(OrderState::Expired),
            OrderEvent::Replace => Ok(OrderState::Replaced),
        }
    }

    fn update_metadata_for_event(&mut self, event: &OrderEvent, timestamp_ns: u64) {
        self.metadata.last_event_timestamp_ns = timestamp_ns;

        match event {
            OrderEvent::PartialFill { filled_qty, fill_price } => {
                let total = self.metadata.total_filled_qty + filled_qty;
                let prev_total = self.metadata.avg_fill_price * self.metadata.total_filled_qty;
                let new_total = prev_total + (fill_price * filled_qty);
                self.metadata.avg_fill_price = if total > 0.0 { new_total / total } else { 0.0 };
                self.metadata.total_filled_qty = total;
            }
            OrderEvent::FullFill { filled_qty, fill_price } => {
                let total = self.metadata.total_filled_qty + filled_qty;
                let prev_total = self.metadata.avg_fill_price * self.metadata.total_filled_qty;
                let new_total = prev_total + (fill_price * filled_qty);
                self.metadata.avg_fill_price = if total > 0.0 { new_total / total } else { *fill_price };
                self.metadata.total_filled_qty = total;
            }
            OrderEvent::Reject { reason } => {
                self.metadata.rejection_reason = Some(reason.clone());
            }
            _ => {}
        }
    }

    fn create_behavior_for_state(&self, state: &OrderState) -> Box<dyn OrderStateBehavior> {
        match state {
            OrderState::Pending => Box::new(PendingState),
            OrderState::Submitted => Box::new(SubmittedState),
            OrderState::PartiallyFilled => Box::new(PartiallyFilledState),
            OrderState::Filled => Box::new(TerminalState { name: "Filled" }),
            OrderState::Cancelled => Box::new(TerminalState { name: "Cancelled" }),
            OrderState::Rejected => Box::new(TerminalState { name: "Rejected" }),
            OrderState::Expired => Box::new(TerminalState { name: "Expired" }),
            OrderState::Replaced => Box::new(TerminalState { name: "Replaced" }),
        }
    }
}

impl Default for OrderStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state_is_pending() {
        let sm = OrderStateMachine::new();
        assert_eq!(*sm.current_state(), OrderState::Pending);
        assert!(sm.is_active());
        assert!(!sm.is_terminal());
    }

    #[test]
    fn test_valid_transition_pending_to_submitted() {
        let mut sm = OrderStateMachine::new();
        let result = sm.submit(1_000_000_000);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), OrderState::Submitted);
        assert_eq!(sm.transition_count(), 1);
    }

    #[test]
    fn test_valid_transition_submitted_to_partial_fill() {
        let mut sm = OrderStateMachine::new();
        sm.submit(1_000_000_000).unwrap();
        let result = sm.partial_fill(5.0, 150.0, 2_000_000_000);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), OrderState::PartiallyFilled);
        assert_eq!(sm.metadata().total_filled_qty, 5.0);
        assert_eq!(sm.metadata().avg_fill_price, 150.0);
    }

    #[test]
    fn test_valid_transition_partial_fill_to_full_fill() {
        let mut sm = OrderStateMachine::new();
        sm.submit(1_000_000_000).unwrap();
        sm.partial_fill(5.0, 150.0, 2_000_000_000).unwrap();
        let result = sm.full_fill(5.0, 151.0, 3_000_000_000);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), OrderState::Filled);
        assert_eq!(sm.metadata().total_filled_qty, 10.0);
        assert!((sm.metadata().avg_fill_price - 150.5).abs() < 0.01);
    }

    #[test]
    fn test_valid_transition_submitted_to_filled_directly() {
        let mut sm = OrderStateMachine::new();
        sm.submit(1_000_000_000).unwrap();
        let result = sm.full_fill(10.0, 150.0, 2_000_000_000);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), OrderState::Filled);
    }

    #[test]
    fn test_valid_transition_submitted_to_cancelled() {
        let mut sm = OrderStateMachine::new();
        sm.submit(1_000_000_000).unwrap();
        let result = sm.cancel(2_000_000_000);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), OrderState::Cancelled);
        assert!(sm.is_terminal());
    }

    #[test]
    fn test_valid_transition_pending_to_rejected() {
        let mut sm = OrderStateMachine::new();
        let result = sm.reject("Insufficient funds".to_string(), 1_000_000_000);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), OrderState::Rejected);
        assert_eq!(sm.metadata().rejection_reason, Some("Insufficient funds".to_string()));
    }

    #[test]
    fn test_invalid_transition_filled_to_pending() {
        let mut sm = OrderStateMachine::new();
        sm.submit(1_000_000_000).unwrap();
        sm.full_fill(10.0, 150.0, 2_000_000_000).unwrap();
        let result = sm.submit(3_000_000_000);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.from, OrderState::Filled);
        assert_eq!(err.to, OrderState::Submitted);
    }

    #[test]
    fn test_invalid_transition_cancelled_to_submitted() {
        let mut sm = OrderStateMachine::new();
        sm.submit(1_000_000_000).unwrap();
        sm.cancel(2_000_000_000).unwrap();
        let result = sm.submit(3_000_000_000);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_transition_rejected_to_partial_fill() {
        let mut sm = OrderStateMachine::new();
        sm.reject("Bad order".to_string(), 1_000_000_000).unwrap();
        let result = sm.partial_fill(5.0, 150.0, 2_000_000_000);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_transition_partial_fill_to_submitted() {
        let mut sm = OrderStateMachine::new();
        sm.submit(1_000_000_000).unwrap();
        sm.partial_fill(5.0, 150.0, 2_000_000_000).unwrap();
        let result = sm.submit(3_000_000_000);
        assert!(result.is_err());
    }

    #[test]
    fn test_transition_log_tracks_all_changes() {
        let mut sm = OrderStateMachine::new();
        sm.submit(1_000_000_000).unwrap();
        sm.partial_fill(5.0, 150.0, 2_000_000_000).unwrap();
        sm.full_fill(5.0, 151.0, 3_000_000_000).unwrap();

        let log = sm.transition_log();
        assert_eq!(log.len(), 3);
        assert_eq!(log[0].0, OrderState::Pending);
        assert_eq!(log[0].1, OrderState::Submitted);
        assert_eq!(log[1].1, OrderState::PartiallyFilled);
        assert_eq!(log[2].1, OrderState::Filled);
    }

    #[test]
    fn test_avg_fill_price_weighted_average() {
        let mut sm = OrderStateMachine::new();
        sm.submit(1_000_000_000).unwrap();
        sm.partial_fill(10.0, 100.0, 2_000_000_000).unwrap();
        sm.partial_fill(20.0, 130.0, 3_000_000_000).unwrap();

        let avg = sm.metadata().avg_fill_price;
        assert!((avg - 120.0).abs() < 0.01);
    }

    #[test]
    fn test_terminal_states_have_no_allowed_transitions() {
        for state in &[
            OrderState::Filled,
            OrderState::Cancelled,
            OrderState::Rejected,
            OrderState::Expired,
            OrderState::Replaced,
        ] {
            assert!(state.allowed_transitions().is_empty(), "{:?} should have no allowed transitions", state);
            assert!(state.is_terminal(), "{:?} should be terminal", state);
        }
    }

    #[test]
    fn test_allowed_transitions_matrix() {
        let pending_allowed = OrderState::Pending.allowed_transitions();
        assert_eq!(pending_allowed, &[OrderState::Submitted, OrderState::Rejected]);

        let submitted_allowed = OrderState::Submitted.allowed_transitions();
        assert_eq!(submitted_allowed.len(), 4);
        assert!(submitted_allowed.contains(&OrderState::PartiallyFilled));
        assert!(submitted_allowed.contains(&OrderState::Filled));
        assert!(submitted_allowed.contains(&OrderState::Cancelled));
        assert!(submitted_allowed.contains(&OrderState::Rejected));

        let partial_allowed = OrderState::PartiallyFilled.allowed_transitions();
        assert_eq!(partial_allowed.len(), 3);
        assert!(partial_allowed.contains(&OrderState::PartiallyFilled));
        assert!(partial_allowed.contains(&OrderState::Filled));
        assert!(partial_allowed.contains(&OrderState::Cancelled));
    }
}
