//! Prepared TCP operation contracts plus focused executor modules.
//!
//! The root stays as a facade so direct and socket
//! execution do not regrow into one large implementation bucket.

mod contract;
#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
mod socket;

pub(crate) use contract::{
    LazyTcpRelayCarrier, PreparedTcpConnectOperation, PreparedTcpRelayOperation,
};
#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
pub(crate) use socket::{SocketTcpHandshake, SocketTcpOperation};
