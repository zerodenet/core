//! Reject only a proven new Source admission, never a nested old cleanup fault.
use zero_platform_tokio::contract::{ChannelError, DeliveryState, Impact, Reason, Stage};
pub(crate) fn rejected() -> ChannelError {
    let mut error = ChannelError::new(Stage::Prepare, Reason::Capacity, Impact::Component);
    error.delivery = DeliveryState::NotSubmitted;
    error
}
pub(crate) fn before_work(error: &ChannelError) -> bool {
    error.stage == Stage::Prepare
        && error.reason == Reason::Capacity
        && error.delivery == DeliveryState::NotSubmitted
}
pub(crate) fn source_admission(error: &zero_engine::EngineError) -> bool {
    match error {
        zero_engine::EngineError::Io(source) => source
            .get_ref()
            .and_then(|source| source.downcast_ref::<ChannelError>())
            .is_some_and(before_work),
        _ => false,
    }
}
