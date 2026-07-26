//! Active outbound health, passive relay health, and probe triggers.

mod outbound;
mod passive_relay;
mod probe;

pub use passive_relay::{PassiveRelayHealthKey, PassiveRelayOutcome, PassiveRelaySelection};
pub use probe::{ProbeTrigger, ProbeTriggerRegistry};

pub(crate) use outbound::OutboundHealth;
pub(crate) use passive_relay::{PassiveRelayHealth, PassiveRelayHealthTransition};
