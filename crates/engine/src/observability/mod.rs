//! Event and statistical observability owned by the engine.

mod event_log;
mod stats;

pub use stats::SessionOutcome;

pub(crate) use event_log::{
    active_flow_record, completed_flow_record, EngineEventLog, QueuedEvent, SnapshotBasis,
    Subscriber,
};
pub use event_log::{OriginalEventHistory, RetainedReplay};
pub(crate) use stats::{EngineStats, StatsQueryGuard, StatsQueryView};

pub(crate) mod traffic;
