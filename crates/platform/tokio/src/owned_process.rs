//! A native job original, driven by its caller's already registered actor.
//! Native tree closure and the actor's consumed Scope exit are separate facts.
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::{OwnedProcess, ProcessTreeClosed};
