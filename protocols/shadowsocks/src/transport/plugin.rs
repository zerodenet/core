//! Protocol-owned SIP003/SIP003u process and endpoint plans.
/// Existing SIP003 local startup bound. It is not a Business lifetime timeout.
pub const STARTUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
pub(crate) mod process;
mod resource;
pub use resource::{PluginTermination, ShadowsocksPluginResource};

pub(crate) mod outbound;
pub(crate) mod stream;

#[cfg(all(test, unix))]
#[path = "../../tests/plugin/fixtures.rs"]
mod fixtures;

pub(crate) mod child;
