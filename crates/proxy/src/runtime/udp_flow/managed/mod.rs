//! Neutral execution machinery for resumable managed UDP flows.
//!
//! Concrete resume values remain opaque and are supplied by registered
//! protocol handlers.

#[cfg(feature = "managed-stream-runtime")]
pub(crate) mod bridge;
#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "managed-datagram-runtime"
))]
mod cache;
#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "managed-datagram-runtime"
))]
mod connection;
#[cfg(feature = "managed-datagram-runtime")]
mod datagram;
#[cfg(feature = "managed-datagram-runtime")]
pub(crate) mod datagram_manager;
mod flow;
pub(crate) mod model;
#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "managed-datagram-runtime"
))]
pub(crate) mod state;
#[cfg(feature = "managed-stream-runtime")]
mod stream;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) mod stream_manager;
#[cfg(all(test, feature = "managed-datagram-runtime"))]
mod tests;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use connection::ManagedPacketUdpFlowConnection;
#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "managed-datagram-runtime"
))]
pub(crate) use connection::ManagedTupleUdpFlowConnection;
#[cfg(feature = "managed-datagram-runtime")]
pub(crate) use datagram::ManagedDatagramFlowConnection;
#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "managed-datagram-runtime"
))]
pub(crate) use flow::ManagedExistingFlowForward;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use flow::ManagedRelayStreamCarrier;
pub(crate) use flow::ManagedUdpFlowResume;
#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "managed-datagram-runtime"
))]
pub(crate) use flow::{ManagedUdpFlowKind, ManagedUdpFlowRequest};
#[cfg(feature = "managed-datagram-runtime")]
pub(crate) use model::ManagedDatagramFlowHandler;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use model::ManagedStreamHandlerPair;
#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "managed-datagram-runtime"
))]
pub(crate) use state::{ManagedUdpHandlers, ManagedUdpState};
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use stream_manager::{
    ManagedPacketUdpResume, ManagedPacketUdpResumeConnector, ManagedStreamConnectorParts,
    ManagedTupleUdpResume, ManagedTupleUdpResumeConnector,
};
