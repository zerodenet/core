//! Typed connection construction and reusable packet I/O; no resume registry.
#[cfg(all(
    test,
    any(
        feature = "managed-stream-runtime",
        feature = "managed-datagram-runtime"
    )
))]
pub(crate) mod connection;
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
#[cfg(all(test, feature = "raw-ip-runtime"))]
#[path = "../../packet_flow/tests/bridge/udp_flow.rs"]
pub(crate) mod packet;
#[cfg(all(test, feature = "managed-stream-runtime"))]
pub(crate) use connection::{managed_tuple_udp_connection_from_flow, SharedManagedUdpConnection};
