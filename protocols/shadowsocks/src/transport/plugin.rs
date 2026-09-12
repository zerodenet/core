//! Protocol-owned SIP003/SIP003u process and endpoint plans.
mod inbound;
mod process;
pub use inbound::ShadowsocksInboundPluginPlan;

pub(crate) mod outbound;
pub(crate) mod stream;
