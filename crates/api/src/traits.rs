use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::ApiResult;
use crate::{
    AuthContext, CommandRequest, CommandResponse, EventFilter, EventReplay, Permission,
    PublishResult, QueryRequest, QueryResponse, RawApiEvent,
};

pub trait QueryService {
    fn query(&self, request: QueryRequest) -> ApiResult<QueryResponse>;
}

pub trait CommandService {
    fn execute(&self, command: CommandRequest) -> ApiResult<CommandResponse>;

    /// Execute a command and resolve only after runtime-visible effects have
    /// been acknowledged by the owning service.
    ///
    /// Purely synchronous implementations inherit the immediate default.
    /// Runtime adapters override this for operations such as listener
    /// reconciliation.
    fn execute_acknowledged(
        &self,
        command: CommandRequest,
    ) -> Pin<Box<dyn Future<Output = ApiResult<CommandResponse>> + Send + '_>>
    where
        Self: Sync,
    {
        Box::pin(async move { self.execute(command) })
    }
}

/// A live stream returned by [`EventSource::subscribe`].
///
/// Implementations may prepend a synthetic synchronization event such as
/// `flow.snapshot`, but subsequent items must be live events emitted after the
/// subscription was registered. Historical inspection belongs to
/// [`EventSource::latest`], not to `subscribe`.
pub trait EventStream: Send + 'static {
    /// Block until the next matching event arrives or the source is closed.
    fn recv(&self) -> Option<RawApiEvent>;

    /// Read the next matching event without blocking.
    fn try_recv(&self) -> Option<RawApiEvent>;

    /// Wait up to `timeout` for an event.
    ///
    /// The default keeps the contract runtime-neutral. Channel-backed streams
    /// should override this method so adapters can wait without polling.
    fn recv_timeout(&self, timeout: Duration) -> EventStreamReceive {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(event) = self.try_recv() {
                return EventStreamReceive::Event(Box::new(event));
            }
            let now = Instant::now();
            if now >= deadline {
                return EventStreamReceive::Timeout;
            }
            std::thread::sleep(
                deadline
                    .saturating_duration_since(now)
                    .min(Duration::from_millis(25)),
            );
        }
    }
}

#[derive(Debug)]
pub enum EventStreamReceive {
    Event(Box<RawApiEvent>),
    Timeout,
    Closed,
}

pub trait EventSource {
    type Stream: EventStream;

    /// Register a live event subscription.
    fn subscribe(&self, filter: EventFilter) -> ApiResult<Self::Stream>;

    /// Snapshot of recent events matching the filter.
    fn latest(&self, limit: usize, filter: EventFilter) -> ApiResult<Vec<RawApiEvent>>;

    /// Replay retained events whose sequence is greater than `sequence`.
    ///
    /// Consumers must inspect `has_gap` before applying the returned events.
    fn since(&self, sequence: u64, limit: usize, filter: EventFilter) -> ApiResult<EventReplay>;
}

pub trait EventSink {
    /// Human-readable name for logging and debugging.
    fn name(&self) -> &str;

    /// Publish a single event to this sink.
    fn publish(&self, event: &RawApiEvent) -> ApiResult<PublishResult>;

    /// Flush any buffered events.
    fn flush(&self) -> ApiResult<()> {
        Ok(())
    }

    /// Optional event type filter for this sink. If `None`, accepts all events.
    fn filter(&self) -> &Option<HashSet<String>> {
        &None
    }

    /// Preferred batch size for this sink. Default 1 (no batching).
    fn batch_size(&self) -> usize {
        1
    }
}

pub trait ApiCodec {
    fn encode<T: Serialize>(&self, value: &T) -> ApiResult<Vec<u8>>;

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> ApiResult<T>;
}

pub trait ApiAuth {
    fn authorize(&self, context: &AuthContext, required: Permission) -> ApiResult<()>;
}
