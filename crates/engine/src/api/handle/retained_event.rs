//! Move the actual queue element and its existing reservation through encoding.
use super::*;
pub struct RetainedEvent(Original);
enum Original {
    Queued(crate::observability::QueuedEvent),
    Initial(RawApiEvent),
}
impl std::ops::Deref for RetainedEvent {
    type Target = RawApiEvent;
    fn deref(&self) -> &RawApiEvent {
        match &self.0 {
            Original::Queued(event) => &event.event,
            Original::Initial(event) => event,
        }
    }
}
impl serde::Serialize for RetainedEvent {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&**self, serializer)
    }
}
impl RetainedEvent {
    pub(crate) fn from_queued(event: crate::observability::QueuedEvent) -> Self {
        Self(Original::Queued(event))
    }
    pub(super) fn into_event(self) -> RawApiEvent {
        match self.0 {
            Original::Queued(event) => event.into_event(),
            Original::Initial(event) => event,
        }
    }
}
impl EventSubscriber {
    fn retained_initial(&self) -> Option<RetainedEvent> {
        self.initial
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
            .map(|event| RetainedEvent(Original::Initial(event)))
    }
    /// Receive the same immutable original, retaining its subscriber byte
    /// reservation until the actual receiver has finished using it. No payload
    /// clone or early dequeue refund occurs. This grants no IO/Scope authority.
    pub fn try_recv_retained(&self) -> Option<RetainedEvent> {
        if let Some(initial) = self.retained_initial() {
            return Some(initial);
        }
        loop {
            let original = self.rx.try_recv().ok()?;
            if matches_event(&original.event, &self.filter) {
                return Some(RetainedEvent::from_queued(original));
            }
        }
    }
    pub fn recv_retained_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<RetainedEvent, mpsc::RecvTimeoutError> {
        if let Some(initial) = self.retained_initial() {
            return Ok(initial);
        }
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(mpsc::RecvTimeoutError::Timeout);
            }
            let original = self.rx.recv_timeout(remaining)?;
            if matches_event(&original.event, &self.filter) {
                return Ok(RetainedEvent::from_queued(original));
            }
        }
    }
}
