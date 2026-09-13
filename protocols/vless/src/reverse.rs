//! VLESS Rvs wire and worker ownership, pinned to Xray-core v26.3.27.
//! Carrier opening and application routing are supplied by the owning runtime.
mod control;
#[cfg(feature = "reality")]
mod portal;
mod state;
mod wire;

pub use control::{Control, ACTIVE, DRAIN};
#[cfg(feature = "reality")]
pub use portal::{Portal, PortalRegistration};
pub use state::{needs_worker, BridgeState, PortalHeartbeat, WorkerLoad};
pub use wire::send_request;

pub const COMMAND: u8 = 4;
pub const REQUEST_DOMAIN: &str = "v1.rvs.cool";
pub const CONTROL_DOMAIN: &str = "reverse";
pub const MONITOR_INTERVAL: core::time::Duration = core::time::Duration::from_secs(2);
pub const PORTAL_IDLE_TIMEOUT: core::time::Duration = core::time::Duration::from_secs(24 * 3600);

#[cfg(feature = "reality")]
mod registry;
#[cfg(feature = "reality")]
pub use registry::PortalRegistry;

#[cfg(feature = "reality")]
pub(crate) mod worker;
