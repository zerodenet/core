mod model;
#[cfg(all(
    test,
    any(
        feature = "managed-stream-runtime",
        feature = "managed-datagram-runtime"
    )
))]
mod tuple;

#[cfg(all(
    test,
    any(
        feature = "managed-stream-runtime",
        feature = "managed-datagram-runtime"
    )
))]
pub(crate) use model::SharedManagedUdpConnection;
#[cfg(all(
    test,
    any(
        feature = "managed-stream-runtime",
        feature = "managed-datagram-runtime"
    )
))]
pub(crate) use tuple::{managed_tuple_udp_connection_from_flow, ManagedTupleUdpFlowConnection};
