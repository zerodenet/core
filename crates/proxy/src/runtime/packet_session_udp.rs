//! Neutral packet-session UDP runtime glue for stream and MUX transports.
//!
//! The root stays as a facade so stream- and mux-carried UDP reuse the same
//! runtime loop without turning this module back into a grab-bag implementation
//! surface.

mod contract;
mod lifecycle;

pub(crate) use contract::{
    PacketSessionUdpFailurePolicy, PacketSessionUdpHandler, PacketSessionUdpReadFailure,
    PacketSessionUdpReadFailureAction, PacketSessionUdpReadResult, PacketSessionUdpRelayRequest,
};
pub(crate) use lifecycle::{
    run_packet_session_udp_relay, run_packet_session_udp_relay_with_dispatch,
    PacketSessionUdpLoopExit,
};
