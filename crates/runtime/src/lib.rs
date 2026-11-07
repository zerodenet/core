//! Common runtime ownership. Protocol implementations and forwarding depend on contracts.
mod apply_owner;
pub mod component;
pub mod configuration;
mod embedded_commands;
mod host;
pub use embedded_commands::{
    AsyncCommandPanic, BlockingCommandFaults, EmbeddedCommandOwner,
    EmbeddedFaultSet as EmbeddedCommandFailures, EmbeddedFaults as EmbeddedCommandFailure,
    FrontendCommand,
};
mod original_budgets;
pub use apply_owner::{
    ApplyObservation, ApplyObservationGuard, ApplyOwner, BlockingCommandOutcome,
    CommandCleanupAccepted, CommandCleanupKind, CommandInvocationFault, CommandInvocationFaults,
};
pub use host::RuntimeHost;
pub use original_budgets::HostOriginalBudgets;

mod probe_owner;
pub use probe_owner::{
    reserve_probe, ProbeAdmission, ProbeCall, ProbeOwnershipFailure, MAX_PROBE_OPERATIONS,
};
