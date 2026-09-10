//! Protocol registry - eliminates per-protocol match arms in the proxy.
//!
//! Each registered protocol contributes focused capability traits for support
//! metadata, inbound listeners, TCP outbound, UDP flows, and packet-path roles.
//! The `ProtocolRegistry` collects capability objects at startup and replaces
//! hard-coded match statements in `ProtocolInventory`.

mod capability;
#[cfg(any(
    feature = "tcp-tunnel-runtime",
    feature = "tcp-session-runtime",
    feature = "tcp-transport-session-runtime"
))]
mod claim;
mod context;
mod defaults;
mod model;
mod registry;
mod transport_leaf;

#[cfg(any(
    feature = "managed-datagram-runtime",
    feature = "managed-stream-runtime"
))]
pub(crate) use capability::ManagedUdpHandlerProvider;
#[cfg(feature = "upstream-association-runtime")]
pub(crate) use capability::UpstreamUdpHandlerProvider;
pub(crate) use capability::{
    ClaimedTcpOutboundLeaf, InboundListenerCapability, OutboundLeafClaim, OutboundLeafInput,
    ProtocolSupportCapability, TcpOutboundCapability,
};
#[cfg(feature = "udp-runtime")]
pub(crate) use capability::{
    ClaimedUdpFlowLeaf, ClaimedUdpPacketPathLeaf, UdpFlowCapability, UdpPacketPathCapability,
};
#[cfg(feature = "tcp-transport-session-runtime")]
pub(crate) use claim::claim_session_tcp_leaf;
#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
pub(crate) use claim::{claim_socket_tcp_leaf, claim_socket_tcp_leaf_with_relay};
pub(crate) use context::{OutboundAdapterContext, TcpRuntimeServices, UpstreamConnectServices};
#[cfg(feature = "udp-runtime")]
pub(crate) use context::{
    UdpAdapterContext, UdpAssociationCloseKind, UdpNetworkServices, UdpRuntimeServices,
};
pub(crate) use defaults::{bind_tcp_inbound, inbound_listen_addr};
pub(crate) use model::{BoundInbound, OutboundLeafRuntime};
pub(crate) use registry::ClaimedOutboundLeaf;
pub(crate) use registry::ProtocolRegistry;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use transport_leaf::claim_relay_two_stream_transport_udp_leaf;
#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
pub(crate) use transport_leaf::claim_transport_tcp_leaf;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use transport_leaf::claim_transport_udp_leaf;
