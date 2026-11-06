//! Common runtime ownership. Protocol implementations and forwarding depend on contracts.
mod apply_owner;
pub mod component;
pub mod configuration;
mod host;
mod original_budgets;
pub use apply_owner::ApplyOwner;
pub use host::RuntimeHost;
pub use original_budgets::HostOriginalBudgets;

mod probe_owner;
pub use probe_owner::{reserve_probe, ProbeAdmission, ProbeCall, MAX_PROBE_OPERATIONS};
