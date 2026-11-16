//! Typed connection construction and reusable packet I/O; no resume registry.
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
#[cfg(all(test, feature = "managed-datagram-runtime"))]
mod tests;
#[cfg(all(
    test,
    any(
        feature = "managed-stream-runtime",
        feature = "managed-datagram-runtime"
    )
))]
pub(crate) use connection::ManagedTupleUdpFlowConnection;
#[cfg(feature = "managed-datagram-runtime")]
pub(crate) use datagram::{ManagedDatagramFlowConnection, ManagedDatagramPendingResponse};
#[cfg(all(test, feature = "raw-ip-runtime"))]
#[path = "../../packet_flow/tests/bridge/udp_flow.rs"]
pub(crate) mod packet;
#[cfg(all(test, feature = "managed-stream-runtime"))]
pub(crate) use connection::{managed_tuple_udp_connection_from_flow, SharedManagedUdpConnection};
