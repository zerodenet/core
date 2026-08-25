//! Proxy transport facade.
//!
//! This module re-exports neutral stream and QUIC contracts from
//! `zero-transport` and owns proxy-specific direct connect, TCP outbound
//! normalization, relay-chain handoff, metering, and rate-limited relay glue.
//! Concrete carrier implementations remain in the `zero-transport` crate.

mod direct;
mod rate_limit;
mod tcp_outbound;
mod tcp_relay;
#[cfg(test)]
mod tests;

pub(crate) use direct::{DirectConnector, DirectTcpConnectFailure, DirectTcpConnection};
pub(crate) use rate_limit::SharedRateLimiter;
pub(crate) use tcp_outbound::{
    extract_tcp_stream, is_block_error, EstablishedTcpOutbound, TcpOutboundFailure, TcpRouteResult,
};
pub(crate) use tcp_relay::copy_one_way;
pub(crate) use tcp_relay::relay_bidirectional_metered;
pub(crate) use tcp_relay::relay_bidirectional_metered_throttled;
pub(crate) use zero_transport::ClientStream;
pub(crate) use zero_transport::MeteredStream;
pub(crate) use zero_transport::PrefixedSocket;
pub(crate) use zero_transport::RecordingStream;
#[cfg(feature = "udp-runtime")]
pub(crate) use zero_transport::RelayCarrier;
pub(crate) use zero_transport::ReplayStream;
#[cfg(feature = "udp-runtime")]
pub(crate) use zero_transport::StreamTraffic;
pub(crate) use zero_transport::TcpRelayStream;

// Re-export transport implementations from zero-transport.
// Only items used directly by proxy code are listed.
#[cfg(feature = "transport_quic")]
pub(crate) use zero_transport::quic::QuicInbound;
#[cfg(feature = "transport_quic")]
pub(crate) use zero_transport::quic::QuicStream;
