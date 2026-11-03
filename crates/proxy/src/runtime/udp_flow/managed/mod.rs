//! Typed connection construction and reusable packet I/O; no resume registry.
#[cfg(feature = "managed-stream-runtime")]
pub(crate) mod bridge;
#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "managed-datagram-runtime"
))]
pub(crate) mod cache;
#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "managed-datagram-runtime"
))]
pub(crate) mod connection;
#[cfg(feature = "managed-datagram-runtime")]
pub(in crate::runtime) mod datagram;
#[cfg(feature = "managed-datagram-runtime")]
pub(crate) mod datagram_manager;
mod flow;
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
#[cfg(feature = "raw-ip-runtime")]
pub(crate) mod packet;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use connection::{managed_tuple_udp_connection_from_flow, SharedManagedUdpConnection};
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use stream_manager::{
    ManagedPacketUdpResume, ManagedPacketUdpResumeConnector, ManagedStreamConnectorFlow,
    ManagedTupleUdpResume, ManagedTupleUdpResumeConnector,
};
