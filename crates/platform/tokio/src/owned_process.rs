//! A native job original, driven by its caller's already registered actor.
//! Native tree closure and the actor's consumed Scope exit are separate facts.
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::{OwnedProcess, ProcessTreeClosed};
#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::{OwnedProcess, ProcessTreeClosed};
#[cfg(any(target_os = "linux", windows))]
mod budget;
#[cfg(any(target_os = "linux", windows))]
pub use budget::{ProcessBudget, ProcessBudgetLimits, ProcessBudgetSnapshot};
