use std::collections::VecDeque;
use std::sync::{mpsc, Mutex};

use zero_api::{
    ApiResult, CommandRequest, CommandResponse, CommandService, EventFilter, EventSource,
    EventStream, QueryRequest, QueryResponse, QueryService, RawApiEvent,
};

use crate::error::EngineError;
use crate::runtime::Engine;

/// In-process handle providing `QueryService`, `CommandService`, and
/// `EventSource` access to the engine.
///
/// Multiple subscribers can receive events concurrently via
/// `EventSource::subscribe`. Slow subscribers retain their registration;
/// the bounded live queue can shed samples while the event-log sequence
/// still exposes the gap to consumers.
#[derive(Clone)]
pub struct EngineHandle {
    engine: Engine,
}

impl EngineHandle {
    pub fn new(engine: Engine) -> Self {
        Self { engine }
    }

    /// Convenience constructor that loads configuration from `path`.
    pub fn from_path(path: impl AsRef<std::path::Path>) -> Result<Self, EngineError> {
        Engine::from_path(path).map(Self::new)
    }

    /// Access the underlying `Engine`.
    pub fn inner(&self) -> &Engine {
        &self.engine
    }

    /// Push an event to all active subscribers.
    ///
    /// Receiver retirement removes its exact logical registration immediately;
    /// already transferred event originals retain their own reservation.
    pub fn emit(&self, event: RawApiEvent) {
        self.engine.emit_event(event);
    }

    /// Production control adapters register this exact live receiver before
    /// encoding their prepaid original initial flow snapshot. The returned
    /// flag selects that existing snapshot responsibility, never closure/IO.
    pub fn subscribe_control_live(
        &self,
        filter: EventFilter,
    ) -> ApiResult<(EventSubscriber, bool)> {
        let initial = wants_flow_snapshot(&filter);
        Ok((
            EventSubscriber::subscribe_live(&self.engine, filter)?,
            initial,
        ))
    }
}

impl QueryService for EngineHandle {
    fn query(&self, request: QueryRequest) -> ApiResult<QueryResponse> {
        self.engine.query(request)
    }
}

impl CommandService for EngineHandle {
    fn execute(&self, command: CommandRequest) -> ApiResult<CommandResponse> {
        self.engine.execute(command)
    }
}

impl EventSource for EngineHandle {
    type Stream = EventSubscriber;

    fn subscribe(&self, filter: EventFilter) -> ApiResult<Self::Stream> {
        EventSubscriber::subscribe(&self.engine, filter)
    }

    fn latest(&self, limit: usize, filter: EventFilter) -> ApiResult<Vec<RawApiEvent>> {
        self.engine.latest(limit, filter)
    }

    fn since(
        &self,
        sequence: u64,
        limit: usize,
        filter: EventFilter,
    ) -> ApiResult<zero_api::EventReplay> {
        Ok(self.engine.events_since(sequence, limit, &filter))
    }
}

/// A single event subscriber created by `EngineHandle::subscribe`.
///
/// Iterate over it with `recv()` / `try_recv()` to read events. Only
/// events matching the subscriber's filter are yielded; others are
/// silently skipped.
pub struct EventSubscriber {
    initial: Mutex<VecDeque<initial::InitialEvent>>,
    rx: mpsc::Receiver<crate::observability::QueuedEvent>,
    registration: crate::observability::SubscriberRegistration,
}

impl EventSubscriber {
    pub(crate) fn subscribe(engine: &Engine, filter: EventFilter) -> ApiResult<Self> {
        let mut subscriber = Self::subscribe_live(engine, filter)?;
        if wants_flow_snapshot(subscriber.registration.filter()) {
            let initial =
                initial::prepare(engine, &subscriber.registration).map_err(subscription_error)?;
            subscriber.initial = Mutex::new(VecDeque::from([initial]));
        }
        Ok(subscriber)
    }
    fn subscribe_live(engine: &Engine, filter: EventFilter) -> ApiResult<Self> {
        // Original book/storage precedes this exact physical channel allocation.
        let prepared = engine
            .prepare_event_subscriber(filter)
            .map_err(subscription_error)?;
        let (tx, rx) = mpsc::sync_channel(1024);
        let registration = prepared.install(tx).map_err(subscription_error)?;
        Ok(Self {
            initial: Mutex::new(VecDeque::new()),
            rx,
            registration,
        })
    }

    /// Block until the next matching event arrives.
    ///
    /// Returns `None` when the publisher has been dropped (the
    /// `EngineHandle` no longer exists).
    pub fn recv(&self) -> Option<RawApiEvent> {
        if let Some(event) = self
            .initial
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .pop_front()
        {
            return Some(event.event);
        }
        loop {
            let event = self.rx.recv().ok()?.into_event();
            if matches_event(&event, self.registration.filter()) {
                return Some(event);
            }
        }
    }

    /// Non-blocking read of the next matching event.
    pub fn try_recv(&self) -> Option<RawApiEvent> {
        if let Some(event) = self
            .initial
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .pop_front()
        {
            return Some(event.event);
        }
        loop {
            let event = self.rx.try_recv().ok()?.into_event();
            if matches_event(&event, self.registration.filter()) {
                return Some(event);
            }
        }
    }
}

impl EventStream for EventSubscriber {
    fn recv(&self) -> Option<RawApiEvent> {
        EventSubscriber::recv(self)
    }
    fn try_recv(&self) -> Option<RawApiEvent> {
        EventSubscriber::try_recv(self)
    }
    fn recv_timeout(&self, timeout: std::time::Duration) -> zero_api::EventStreamReceive {
        match self.recv_retained_timeout(timeout) {
            Ok(event) => zero_api::EventStreamReceive::Event(Box::new(event.into_event())),
            Err(mpsc::RecvTimeoutError::Timeout) => zero_api::EventStreamReceive::Timeout,
            Err(mpsc::RecvTimeoutError::Disconnected) => zero_api::EventStreamReceive::Closed,
        }
    }
}
mod retained_event;
pub use retained_event::RetainedEvent;

fn wants_flow_snapshot(filter: &EventFilter) -> bool {
    filter.event_types.is_empty()
        || filter.event_types.iter().any(|event_type| {
            matches!(
                event_type.as_str(),
                zero_api::event_type::FLOW_STARTED
                    | zero_api::event_type::FLOW_ROUTED
                    | zero_api::event_type::FLOW_UPDATED
                    | zero_api::event_type::FLOW_COMPLETED
                    | zero_api::event_type::FLOW_SNAPSHOT
            )
        })
}

fn matches_event(event: &RawApiEvent, filter: &EventFilter) -> bool {
    if !filter.event_types.is_empty()
        && !filter
            .event_types
            .iter()
            .any(|event_type| event_type == &event.event_type)
    {
        return false;
    }
    true
}

fn subscription_error(error: std::io::Error) -> zero_api::ApiError {
    let code = match error.kind() {
        std::io::ErrorKind::WouldBlock => zero_api::ApiErrorCode::Conflict,
        std::io::ErrorKind::InvalidInput => zero_api::ApiErrorCode::InvalidArgument,
        _ => zero_api::ApiErrorCode::Internal,
    };
    zero_api::ApiError::new(code, error.to_string())
}

mod initial;
