//! Unified VLESS transport builder.
//!
//! Wraps a raw TCP socket with the configured VLESS transport layer
//! (TLS / Reality / WebSocket / gRPC / H2), dispatching to the correct
//! connect function for every valid combination.

mod inbound;
mod leaf;
mod managed_udp;
mod options;
mod outbound;
mod profile;
pub(crate) use profile::client_tls_for_stream;
mod runtime;

pub use inbound::{
    VlessInboundBindPlan, VlessInboundListenerRequest, VlessInboundStreamMetadata,
    VlessInboundTransportStreams, VlessStandardTcpAccept,
};
pub use leaf::{VlessOutboundLeaf, VlessReverseBridge};
pub use managed_udp::{VlessManagedUdpConnectorFlow, VlessManagedUdpFlowResume};
pub use options::{
    VlessInboundOptionsRef, VlessInboundUserRef, VlessOutboundBuildOptionsRef,
    VlessOutboundOptionsRef, VlessQuicBindOptionsRef, VlessQuicClientOptionsRef,
    VlessRealityClientOptionsRef, VlessRealityServerOptionsRef, VlessXhttpDownloadOptionsRef,
};
pub use runtime::VlessTransportRuntime;
