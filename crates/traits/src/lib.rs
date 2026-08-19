#![no_std]
#![allow(async_fn_in_trait)]

extern crate alloc;

pub mod outbound_leaf;
pub mod protocol;
pub mod udp_flow;

pub use outbound_leaf::ProtocolOutboundLeaf;
pub use udp_flow::{ProtocolRelayTwoStreamUdpFlowLeaf, ProtocolUdpFlowLeaf};

use alloc::{sync::Arc, vec::Vec};
use core::sync::atomic::{AtomicU8, Ordering};
pub use protocol::{
    ClientTlsProfile, DatagramCodec, DeferredTcpTunnelProtocol, GrpcTransportProfile,
    H2TransportProfile, HttpUpgradeTransportProfile, InboundFallbackProfile,
    ProtocolCapabilityDescriptor, ProtocolCapabilityLevel, ProtocolCapabilityState,
    ProtocolMetadata, ProtocolNetworkCapability, ServerTlsProfile, SplitHttpTransportProfile,
    StreamMuxTransportHints, TcpSessionProtocol, TcpTunnelProtocol, UdpDatagramFraming,
    UdpPacketFraming, UdpPacketPath, UdpPacketStreamFraming, UdpPacketTunnelProtocol,
    UdpRelayProtocol, WebSocketTransportProfile,
};
pub use protocol::{InboundTransport, TransportKind};

// Address types

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IpAddress {
    V4([u8; 4]),
    V6([u8; 16]),
}

impl IpAddress {
    pub fn is_v4(&self) -> bool {
        matches!(self, IpAddress::V4(_))
    }

    pub fn is_v6(&self) -> bool {
        matches!(self, IpAddress::V6(_))
    }
}

/// Network socket address: an IP address and port.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SocketAddress {
    pub ip: IpAddress,
    pub port: u16,
}

impl SocketAddress {
    pub const fn new(ip: IpAddress, port: u16) -> Self {
        Self { ip, port }
    }
}

// I/O traits

const READ_BYPASS: u8 = 0x01;
const WRITE_BYPASS: u8 = 0x02;

/// Optional control surface for a stream transport that can hand its raw
/// carrier back to the protocol after an authenticated in-band transition.
///
/// The control is deliberately transport-neutral. Protocols may request the
/// transition, while the owning transport decides whether and how it can be
/// performed without losing buffered bytes.
#[derive(Debug, Clone, Default)]
pub struct TransportBypassControl {
    state: Arc<AtomicU8>,
}

impl TransportBypassControl {
    pub fn request_read_bypass(&self) {
        self.state.fetch_or(READ_BYPASS, Ordering::Release);
    }

    pub fn request_write_bypass(&self) {
        self.state.fetch_or(WRITE_BYPASS, Ordering::Release);
    }

    pub fn read_bypass_requested(&self) -> bool {
        self.state.load(Ordering::Acquire) & READ_BYPASS != 0
    }

    pub fn write_bypass_requested(&self) -> bool {
        self.state.load(Ordering::Acquire) & WRITE_BYPASS != 0
    }
}

pub trait AsyncSocket: Send + Sync + Unpin {
    type Error;

    /// Returns a transport-owned mode control when the carrier supports an
    /// authenticated transition to its underlying raw byte stream.
    fn transport_bypass_control(&self) -> Option<TransportBypassControl> {
        None
    }

    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl core::future::Future<Output = Result<usize, Self::Error>> + Send + 'a;
    fn write_all<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl core::future::Future<Output = Result<(), Self::Error>> + Send + 'a;
    fn shutdown<'a>(
        &'a mut self,
    ) -> impl core::future::Future<Output = Result<(), Self::Error>> + Send + 'a;
}

pub trait TcpListener: Send + Sync {
    type Stream: AsyncSocket;
    type Error;

    /// Accept a stream together with its complete remote socket identity.
    ///
    /// The source port is part of a TCP flow's identity and must not be
    /// discarded by runtime-neutral listener implementations.
    async fn accept(&self) -> Result<(Self::Stream, Option<SocketAddress>), Self::Error>;
}

pub trait DatagramSocket: Send + Sync + Unpin {
    type Error;

    async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, IpAddress, u16), Self::Error>;
    async fn send_to(&self, buf: &[u8], addr: IpAddress, port: u16) -> Result<(), Self::Error>;
}

// Network stack traits

/// Error from a network stack operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StackError {
    /// Generic I/O failure.
    Io,
    /// The stack has been shut down.
    Closed,
    /// Malformed or unsupported packet.
    InvalidPacket,
}

/// TCP connection acceptor backed by raw IP packets.
///
/// Implementations may use a user-space state machine, OS-level
/// redirection, or a hybrid approach.  The consumer reads raw packets
/// from a TUN device (or equivalent) and feeds them via [`feed`];
/// fully-handshaked connections become available via [`accept`].
///
/// The `Connection` type should implement `tokio::io::AsyncRead +
/// AsyncWrite + Unpin + Send` for integration with the proxy pipeline.
/// That bound is expressed at the *consumer* site (via `where`), keeping
/// this trait runtime-agnostic.
///
/// [`feed`]: TcpStack::feed
/// [`accept`]: TcpStack::accept
pub trait TcpStack: Send + Sync {
    /// Established TCP connection.  Implementations should also
    /// implement `AsyncRead + AsyncWrite + Unpin + Send` from the
    /// chosen async runtime.
    type Connection: Send + 'static;

    /// Feed a raw IP packet into the stack.
    ///
    /// Non-TCP packets are silently ignored.  The stack parses the
    /// TCP header, updates internal connection state, and may emit
    /// response packets (SYN-ACK, ACK, FIN, etc.) through an internal
    /// outbound channel.
    async fn feed(&self, packet: &[u8]);

    /// Accept the next fully-established TCP connection.
    ///
    /// Returns the connection stream together with the source (client)
    /// and destination (server) addresses, or `None` when the stack
    /// has been shut down.
    async fn accept(&self) -> Option<(Self::Connection, SocketAddress, SocketAddress)>;
}

/// UDP datagram handler backed by raw IP packets.
///
/// Works like [`TcpStack`] but for connectionless UDP datagrams:
/// inbound packets are queued via [`feed`] and consumed via
/// [`recv_from`]; outbound datagrams are sent via [`send_to`].
///
/// [`feed`]: UdpStack::feed
/// [`recv_from`]: UdpStack::recv_from
/// [`send_to`]: UdpStack::send_to
pub trait UdpStack: Send + Sync {
    /// Feed a raw IP packet into the stack.
    ///
    /// Non-UDP packets are silently ignored.
    async fn feed(&self, packet: &[u8]);

    /// Receive the next queued UDP datagram.
    ///
    /// Returns `(bytes_read, source, destination)`, or `None` when
    /// the stack has been shut down.
    async fn recv_from(&self, buf: &mut [u8]) -> Option<(usize, SocketAddress, SocketAddress)>;

    /// Send a UDP datagram from `src` to `dst`.
    ///
    /// The stack builds the IP + UDP headers (including checksums)
    /// and emits the raw packet through its internal outbound channel.
    async fn send_to(&self, data: &[u8], src: SocketAddress, dst: SocketAddress);
}

/// Complete network stack: TCP streams + UDP datagrams.
///
/// Convenience trait that bundles a [`TcpStack`] and a [`UdpStack`]
/// behind a single type.  Implementations choose the strategy
/// (user-space, system-level, or mixed).
pub trait NetworkStack: Send + Sync + 'static {
    type Tcp: TcpStack;
    type Udp: UdpStack;

    fn tcp(&self) -> &Self::Tcp;
    fn udp(&self) -> &Self::Udp;
}

// Other abstractions

pub trait DnsResolver: Send + Sync {
    type Error;

    async fn resolve(&self, domain: &str) -> Result<Vec<IpAddress>, Self::Error>;
}

pub trait TlsConnector: Send + Sync {}

pub trait TlsAcceptor: Send + Sync {}

pub trait CryptoProvider: Send + Sync {}

pub trait TimeProvider: Send + Sync {
    fn now_unix_seconds(&self) -> u64;
}

pub trait Allocator: Send + Sync {}
