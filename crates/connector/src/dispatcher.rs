use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use tracing::warn;
use zero_api::{
    event_type, EventFilter, EventSource, EventStream, EventStreamReceive, RawApiEvent, SinkStatus,
};
use zero_config::{ApiConfig, EventDispatcherConfig};

use crate::network::EventDispatcherNetwork;
use crate::registry::{build_event_sinks, resolve_path, ConfiguredEventSink};
use crate::resources::{DispatcherResources, RetainedDeadLetter};
use crate::{ConnectorError, ConnectorResult};

mod admission;
pub(crate) mod children;
mod continuation;
mod delivery;
mod failure;
mod handle;
mod initialization;
mod memory;
pub(crate) mod outbox;
mod runner;
mod source;
mod spawn;
mod stats;
mod types;
mod worker;

#[cfg(test)]
use admission::enqueue_sample;
use admission::{dispatch_event, fill_pending_from_outbox, is_discardable_sample};
use children::DispatcherChildren;
pub use continuation::{resume_event_dispatcher_with_network, EventDispatcherContinuation};
use continuation::{ContinuationState, ShutdownMode};
use delivery::{drain_worker_results, next_wait_interval, submit_pending};
pub use failure::DispatcherRetirementFailure;
use initialization::InitSignal;
use outbox::{DeliveryOutbox, OutboxDelivery};
use runner::run_event_dispatcher;
use source::drain_event_source;
#[cfg(feature = "kernel-events")]
pub use spawn::prepare_kernel_event_dispatcher_paused_with_network;
use spawn::spawn_event_dispatcher_inner;
pub use spawn::{
    prepare_event_dispatcher_paused_with_network, spawn_event_dispatcher,
    spawn_event_dispatcher_with_engine_started,
    spawn_event_dispatcher_with_engine_started_and_network, spawn_event_dispatcher_with_network,
};
use stats::{
    record_delivery, record_dispatcher_failure, record_dispatcher_replay_gap,
    record_outbox_ack_failure, record_outbox_ack_recovered, record_outbox_failure,
    refresh_pending_stats, PerSinkStats,
};
use types::{
    finish_delivery, persist_delivery, retry_ack, retry_delay, BlockedEvent, PendingDelivery,
};
use worker::{SinkPublishResult, SinkWorker, SinkWorkerResult};

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);

type DispatcherTaskExit = Result<Option<EventDispatcherContinuation>, Arc<tokio::task::JoinError>>;
type SharedSinkStats = Arc<Mutex<Vec<(String, Arc<PerSinkStats>)>>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventDispatcherOptions {
    pub poll_interval: Duration,
    /// Test/embedding override. Zero uses `api.dispatcher.max_retry_attempts`
    /// when this value is zero.
    pub max_retry_attempts: u32,
}

impl Default for EventDispatcherOptions {
    fn default() -> Self {
        Self {
            poll_interval: DEFAULT_POLL_INTERVAL,
            max_retry_attempts: 0,
        }
    }
}

pub struct EventDispatcherHandle {
    shutdown: Option<tokio::sync::oneshot::Sender<ShutdownMode>>,
    task: Option<JoinHandle<Option<EventDispatcherContinuation>>>,
    task_charge: Option<crate::capacity::TaskCharge>,
    task_exit: Option<DispatcherTaskExit>,
    children: Arc<DispatcherChildren>,
    resources: DispatcherResources,
    init_rx: Option<mpsc::Receiver<ConnectorResult<bool>>>,
    init_notify: Arc<tokio::sync::Notify>,
    init_result: Option<Result<bool, Arc<ConnectorError>>>,
    activate: Option<tokio::sync::oneshot::Sender<()>>,
    /// Per-sink delivery stats shared with the dispatcher thread.
    sink_stats: SharedSinkStats,
}

#[derive(Clone)]
pub struct EventDispatcherStatusHandle {
    sink_stats: SharedSinkStats,
}
