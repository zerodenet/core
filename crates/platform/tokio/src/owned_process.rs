//! A native job original, driven by its caller's already registered actor.
//! Native process closure and the actor's consumed Scope exit are separate facts.
//! Non-Linux Unix attests its actual child only; Linux/Windows retain tree containment.
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::{OwnedProcess, ProcessTreeClosed};
#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::{OwnedProcess, ProcessTreeClosed};
#[cfg(any(unix, windows))]
mod budget;
#[cfg(any(unix, windows))]
pub use budget::{PrepaidProcessJob, ProcessBudget, ProcessBudgetLimits, ProcessBudgetSnapshot};

#[cfg(all(unix, not(target_os = "linux")))]
mod unix_child;
#[cfg(all(unix, not(target_os = "linux")))]
pub use unix_child::{OwnedProcess, ProcessChildClosed};

mod endpoint_spec;
pub use endpoint_spec::ProcessEndpointSpec;
#[cfg(any(unix, windows))]
mod endpoint;
#[cfg(any(unix, windows))]
pub use endpoint::{
    ProcessEndpointClosed, ProcessEndpointDemand, ProcessEndpointHandoff,
    ProcessEndpointRetirement, ProcessEndpointUse, ProcessProbe, ProcessProbeClosed,
    ProcessReservationsClosed,
};

#[cfg(any(unix, windows))]
pub(crate) use endpoint::ProcessEndpointFence;
