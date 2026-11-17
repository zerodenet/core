//! Explicit test fixtures for the retired pre-standard packet-session loop.
//! Production Stream/MUX Datagram delivery runs through the actual standard Source.

mod contract;
mod lifecycle;

pub(crate) use contract::{
    PacketSessionUdpFailurePolicy, PacketSessionUdpHandler, PacketSessionUdpReadFailure,
    PacketSessionUdpReadResult, PacketSessionUdpRelayRequest,
};
pub(crate) use lifecycle::{run_packet_session_udp_relay_with_dispatch, PacketSessionUdpLoopExit};
