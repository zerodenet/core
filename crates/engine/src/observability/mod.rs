//! Event and statistical observability owned by the engine.

mod event_log;
mod stats;

pub use stats::SessionOutcome;

pub use event_log::RetainedReplay;
pub(crate) use event_log::{
    active_flow_record, completed_flow_record, EngineEventLog, QueuedEvent, Subscriber,
};
pub(crate) use stats::{EngineStats, StatsQueryGuard, StatsQueryView};

pub(crate) mod traffic;
