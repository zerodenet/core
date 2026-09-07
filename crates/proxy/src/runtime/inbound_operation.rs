//! Prepared inbound-listener contracts plus focused listener/context modules.
//!
//! The root stays as a facade so TCP, TCP+datagram, authenticated QUIC, and
//! mixed TCP-or-QUIC execution do not regrow into one large implementation
//! bucket.

mod context;
mod contract;
#[cfg(feature = "authenticated-quic-inbound-runtime")]
mod quic;
#[cfg(feature = "managed-datagram-runtime")]
mod tcp_and_datagram;
#[cfg(feature = "managed-datagram-runtime")]
mod tcp_and_peer_datagram;
mod tcp_listener;
#[cfg(feature = "transport_quic")]
mod tcp_or_quic;

pub(crate) use context::InboundConnectionContext;
pub(crate) use contract::PreparedInboundListenerOperation;
#[cfg(feature = "authenticated-quic-inbound-runtime")]
pub(crate) use quic::{
    AuthenticatedQuicInboundConnection, AuthenticatedQuicInboundListenerOperation,
    AuthenticatedQuicInboundProfile,
};
#[cfg(feature = "managed-datagram-runtime")]
pub(crate) use tcp_and_datagram::TcpAndDatagramInboundListenerOperation;
pub(crate) use tcp_listener::TcpInboundListenerOperation;
#[cfg(feature = "transport_quic")]
pub(crate) use tcp_or_quic::TcpOrQuicInboundListenerOperation;

#[cfg(feature = "managed-datagram-runtime")]
pub(crate) use tcp_and_peer_datagram::TcpAndPeerDatagramInboundListenerOperation;
