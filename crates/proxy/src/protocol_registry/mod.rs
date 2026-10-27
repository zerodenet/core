//! Protocol registry - eliminates per-protocol match arms in the proxy.
//!
//! Migrated protocols expose executable roles through the component registry.
//! This compatibility registry keeps support metadata, real inbound listeners,
//! optional outbound projections and remaining UDP handlers. Registration never
//! requires marker traits for roles an adapter does not implement.

mod capability;
#[cfg(any(
    feature = "tcp-tunnel-runtime",
    feature = "tcp-session-runtime",
    feature = "tcp-transport-session-runtime"
))]
mod claim;
mod context;
mod defaults;
mod endpoint;
pub(crate) use endpoint::{
    EndpointControlCapability, EndpointObservation, EndpointObservationCapability,
};
mod model;
mod registry;
mod transport_leaf;

#[cfg(any(
    feature = "managed-datagram-runtime",
    feature = "managed-stream-runtime"
))]
pub(crate) use capability::ManagedUdpHandlerProvider;
#[cfg(feature = "raw-ip-runtime")]
pub(crate) use capability::{
    ClaimedPacketLeaf, OutboundDeviceCompletion, OutboundDeviceLifecycleCapability,
    PreparedOutboundDeviceState,
};
pub(crate) use capability::{
    ClaimedTcpOutboundLeaf, InboundListenerCapability, OutboundLeafClaim, OutboundLeafInput,
    ProtocolSupportCapability,
};
#[cfg(feature = "udp-runtime")]
pub(crate) use capability::{ClaimedUdpFlowLeaf, ClaimedUdpPacketPathLeaf};
#[cfg(feature = "tcp-transport-session-runtime")]
pub(crate) use claim::claim_session_tcp_leaf_with_source;
#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
pub(crate) use claim::{claim_socket_tcp_leaf, claim_socket_tcp_leaf_with_relay};
pub(crate) use context::{
    OutboundAdapterContext, TcpExecutionServices, TcpRuntimeServices, UpstreamConnectServices,
};
#[cfg(feature = "raw-ip-runtime")]
pub(crate) use context::{OutboundDevicePreparationContext, PreparedDevicePacketPath};
#[cfg(feature = "udp-runtime")]
pub(crate) use context::{
    PacketPathExecutionServices, UdpAdapterContext, UdpAssociationCloseKind,
    UdpAssociationObserver, UdpNetworkServices, UdpRuntimeServices,
};
#[cfg(feature = "udp-runtime")]
pub(crate) use defaults::bind_datagram_listener;
pub(crate) use defaults::prepare_tcp_inbound_bind;
pub(crate) use defaults::{bind_tcp_inbound, bind_tcp_listener, inbound_listen_addr};
pub(crate) use model::{BoundInbound, InboundRecovery, OutboundLeafRuntime, PreparedInboundBind};
pub(crate) use registry::ClaimedOutboundLeaf;
pub(crate) use registry::ProtocolRegistry;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use transport_leaf::claim_relay_two_stream_transport_udp_leaf;
#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
pub(crate) use transport_leaf::claim_transport_tcp_leaf;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use transport_leaf::claim_transport_udp_leaf;

#[cfg(feature = "managed-stream-runtime")]
mod service;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use service::InboundServiceCapability;
