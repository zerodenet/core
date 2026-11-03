//! Protocol registry - eliminates per-protocol match arms in the proxy.
//!
//! Migrated protocols expose executable roles through the component registry.
//! This registry keeps support metadata, standard inbound factories and
//! optional outbound projections. Registration never
//! requires marker traits for roles an adapter does not implement.

mod capability;
mod context;
mod defaults;
mod endpoint;
pub(crate) use endpoint::{
    EndpointControlCapability, EndpointObservation, EndpointObservationCapability,
};
mod model;
mod registry;

pub(crate) use capability::{
    OutboundLeafClaim, OutboundLeafInput, ProtocolSupportCapability, TcpConnectResult,
    TcpRelayResult,
};
#[cfg(feature = "udp-runtime")]
pub(crate) use capability::{UdpFlowResult, UdpRelayResult};
pub(crate) use context::{
    OutboundAdapterContext, TcpExecutionServices, TcpRuntimeServices, UpstreamConnectServices,
};
#[cfg(feature = "udp-runtime")]
pub(crate) use context::{
    PacketPathExecutionServices, UdpAdapterContext, UdpAssociationCloseKind,
    UdpAssociationObserver, UdpNetworkServices, UdpRuntimeServices,
};
#[cfg(all(test, feature = "udp-runtime"))]
pub(crate) use defaults::bind_datagram_listener;
#[cfg(test)]
pub(crate) use defaults::bind_tcp_listener;
pub(crate) use model::{InboundRecovery, OutboundLeafRuntime, PreparedInboundBind};
pub(crate) use registry::ClaimedOutboundLeaf;
pub(crate) use registry::ProtocolRegistry;

#[cfg(feature = "managed-stream-runtime")]
mod service;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use service::InboundServiceCapability;
