//! Protocol-owned SIP003/SIP003u process and endpoint plans.
mod inbound;
mod process;
mod resource;
pub use inbound::ShadowsocksInboundPluginPlan;
pub use resource::{PluginTermination, ShadowsocksPluginResource};

pub(crate) mod outbound;
pub(crate) mod stream;

#[cfg(all(test, unix))]
#[path = "../../tests/plugin/fixtures.rs"]
mod fixtures;
