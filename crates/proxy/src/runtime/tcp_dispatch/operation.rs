//! Prepared TCP operation contracts plus focused executor modules.
//!
//! The root stays as a facade so direct, socket, session, and transport-leaf
//! execution do not regrow into one large implementation bucket.

mod contract;
mod direct;
#[cfg(feature = "tcp-transport-session-runtime")]
mod session;
#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
mod socket;
#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
mod transport;

pub(crate) use contract::{
    LazyTcpRelayCarrier, PreparedTcpConnectOperation, PreparedTcpRelayOperation,
};
pub(crate) use direct::DirectTcpConnectOperation;
#[cfg(feature = "tcp-transport-session-runtime")]
pub(crate) use session::{SessionTcpConnectOperation, SessionTcpHandshake};
#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
pub(crate) use socket::{SocketTcpConnectOperation, SocketTcpHandshake, SocketTcpRelayOperation};
#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
pub(crate) use transport::{TransportLeafTcpConnectOperation, TransportLeafTcpRelayOperation};
