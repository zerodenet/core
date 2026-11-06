#[cfg(feature = "managed-datagram-runtime")]
mod connection;

#[cfg(feature = "managed-datagram-runtime")]
pub(crate) use connection::{
    managed_datagram_connection_from_flow, ManagedDatagramFlowConnection,
    ManagedDatagramPendingResponse,
};
