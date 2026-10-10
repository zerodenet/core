//! Response accounting, delivery models, and lifecycle helpers.

mod accounting;
mod lifecycle;
mod parts;
mod response;

pub(crate) use lifecycle::log_completed_udp_flow;
#[cfg(feature = "upstream-association-runtime")]
pub(crate) use lifecycle::wait_for_upstream_idle;
#[cfg(feature = "upstream-association-runtime")]
pub(crate) use parts::UdpUpstreamResponseParts;
pub(crate) use parts::{UdpChainResponseParts, UdpDirectResponseParts};
#[cfg(feature = "upstream-association-runtime")]
pub(crate) use response::record_upstream_udp_response_received;
pub(crate) use response::{record_chain_udp_response_parts, record_direct_udp_response_parts};

#[cfg(test)]
mod tests;
