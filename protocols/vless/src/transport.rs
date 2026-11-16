//! Unified VLESS transport builder.
//!
//! Wraps a raw TCP socket with the configured VLESS transport layer
//! (TLS / Reality / WebSocket / gRPC / H2), dispatching to the correct
//! connect function for every valid combination.

mod inbound;
mod leaf;
mod managed_udp;
mod navigation;
mod options;
mod outbound;
mod profile;
pub(crate) use outbound::{
    OwnedVlessOutboundTransportPlan, OwnedXhttpHalf, OwnedXhttpKind, OwnedXhttpParts,
};
pub(crate) use profile::{client_tls_for_stream, VlessQuicClientProfile};
mod runtime;

pub use inbound::{
    PreparedVlessInboundListenerRequest, VlessInboundBindPlan, VlessInboundListenerRequest,
    VlessInboundStreamMetadata, VlessInboundTransportStreams,
};
pub use leaf::VlessOutboundLeaf;
pub(crate) use leaf::{OwnedMkcpParts, OwnedQuicParts};
pub use managed_udp::{VlessManagedUdpConnectorFlow, VlessManagedUdpFlowResume};
pub use options::{
    VlessInboundOptionsRef, VlessInboundUserRef, VlessOutboundBuildOptionsRef,
    VlessOutboundOptionsRef, VlessQuicBindOptionsRef, VlessQuicClientOptionsRef,
    VlessRealityClientOptionsRef, VlessRealityServerOptionsRef, VlessXhttpDownloadOptionsRef,
};
pub use runtime::browser_dialer::{
    ProviderFactory as BrowserProviderFactory, ProviderOptions as BrowserProviderOptions,
};
#[cfg(feature = "reality")]
pub use runtime::preconnect::Access as PreconnectPool;
pub use runtime::VlessTransportRuntime;
