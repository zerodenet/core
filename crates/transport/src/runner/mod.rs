//! High-level hosted component runners that encapsulate RuntimeHost lifecycle,
//! admission, cancellation/deadline racing, carrier transport wrapping and loan
//! preservation so protocol crates only implement pure protocol handshakes.

mod call;
#[cfg(any(
    feature = "tls",
    feature = "ws",
    feature = "grpc",
    feature = "h2",
    feature = "http_upgrade"
))]
mod carrier;
mod lifecycle;
mod pooled_role;
mod stateless;

pub use crate::pooled_driver::ExclusiveIo;
pub use call::{FactRecorder, GuardedCall};
#[cfg(any(
    feature = "tls",
    feature = "ws",
    feature = "grpc",
    feature = "h2",
    feature = "http_upgrade"
))]
pub use carrier::{
    connect_stateless_transport_stack, wrap_stateless_carrier, StatelessTransportStack,
};
pub use lifecycle::LifecycleChain;
pub use pooled_role::{
    PooledBoundLeaf, PooledDatagramProtocol, PooledRoleFactory, PooledRoleOptions,
    PooledRoleProtocol, WithPooledDatagram,
};
pub use stateless::{
    DatagramProtocolHandshake, HandshakeError, NoDatagram, StatelessOutboundFactory,
    StatelessOutboundOptions, StreamProtocolHandshake, WithDatagram,
};
