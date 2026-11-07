//! Scoped local endpoint metadata. A lookup is not a Child/job/native egress grant.
use std::net::SocketAddr;
mod lookup;
pub use lookup::{LocalProcessLookup, LocalProcessLookupStart, LocalProcessTransport};
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalProcessInfo {
    pub pid: u32,
    pub name: Option<String>,
    pub path: Option<String>,
}
#[cfg(target_os = "linux")]
#[path = "process/linux.rs"]
mod platform;
#[cfg(windows)]
#[path = "process/windows.rs"]
mod platform;
#[cfg(target_os = "macos")]
#[path = "process/macos.rs"]
mod platform;
#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
#[path = "process/unsupported.rs"]
mod platform;
