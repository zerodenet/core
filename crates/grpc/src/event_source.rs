//! Production subscriptions retain the same Engine queue element through encode.
use super::*;
pub(super) enum Subscriber<S> {
    Engine(zero_engine::EventSubscriber),
    Standalone(S),
}
pub(super) enum Event {
    Original(zero_engine::RetainedEvent),
    Standalone(Box<zero_api::RawApiEvent>),
}
impl std::ops::Deref for Event {
    type Target = zero_api::RawApiEvent;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Original(event) => event,
            Self::Standalone(event) => event,
        }
    }
}
impl<S: EventStream> Subscriber<S> {
    pub(super) fn receive(
        &self,
        timeout: Duration,
    ) -> Result<Event, std::sync::mpsc::RecvTimeoutError> {
        match self {
            Self::Engine(source) => source.recv_retained_timeout(timeout).map(Event::Original),
            Self::Standalone(source) => match source.recv_timeout(timeout) {
                zero_api::EventStreamReceive::Event(event) => Ok(Event::Standalone(event)),
                zero_api::EventStreamReceive::Timeout => {
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout)
                }
                zero_api::EventStreamReceive::Closed => {
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
                }
            },
        }
    }
}
