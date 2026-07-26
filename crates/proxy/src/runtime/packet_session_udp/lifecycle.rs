//! Packet-session UDP relay lifecycle facade.
//!
//! The root stays as a facade so relay orchestration, inbound read dispatch,
//! response handling, failure handling, and feature-specific event loops do not
//! regrow into one large implementation bucket.

mod failure;
mod read;
mod relay;
mod response;
#[cfg(test)]
mod tests;
#[cfg(feature = "upstream-association-runtime")]
mod with_upstream;
#[cfg(not(feature = "upstream-association-runtime"))]
mod without_upstream;

pub(crate) use relay::{
    run_packet_session_udp_relay, run_packet_session_udp_relay_with_dispatch,
    PacketSessionUdpLoopExit,
};
