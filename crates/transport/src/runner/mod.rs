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
mod inbound;
mod lifecycle;
mod pooled_role;
#[cfg(feature = "quic")]
pub mod quic_pool;
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
pub use inbound::{
    StandardTcpInboundFactory, StandardTcpInboundOptions, StandardTcpInboundProtocol,
};
pub use lifecycle::LifecycleChain;
pub use pooled_role::{
    PooledBoundLeaf, PooledDatagramProtocol, PooledRoleFactory, PooledRoleOptions,
    PooledRoleProtocol, WithPooledDatagram,
};
#[cfg(feature = "quic")]
pub use quic_pool::{
    default_metadata_limits, NoQuicPoolDatagram, QuicPool, QuicPoolDatagramProtocol,
    QuicPoolFactory, QuicPoolGeneration, QuicPoolLimits, QuicPoolOptions, QuicPoolProtocol,
    QuicPoolRoleFactory, QuicPoolRoleOptions, QuicPoolRoleProtocol, QuicPooledStream,
};
pub use stateless::{
    DatagramProtocolHandshake, HandshakeError, NoDatagram, StatelessOutboundFactory,
    StatelessOutboundOptions, StreamProtocolHandshake, WithDatagram,
};
