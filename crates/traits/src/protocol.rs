use alloc::string::String;
use alloc::vec::Vec;

use crate::AsyncSocket;

// ── Inbound transport classification ─────────────────────────────────

/// The transport a protocol's inbound listener binds.
///
/// Declared by each protocol adapter so the proxy runtime can dispatch
/// bind/spawn decisions without re-reading the protocol's private config
/// fields. This is the single source of truth for "does this listener
/// open a TCP socket or a QUIC endpoint".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    /// Raw or TLS-over-TCP listener.
    Tcp,
    /// QUIC (UDP) listener (e.g. VLESS/QUIC, Hysteria2).
    Quic,
}

/// A protocol adapter that declares the transport of its inbound listener.
///
/// Adapters implement this so the runtime can classify a listener for
/// bind/spawn dispatch, idle-timeout policy, and capability reporting
/// without matching on the concrete protocol config enum.
pub trait InboundTransport {
    /// The transport kind this adapter's inbound listener uses.
    fn inbound_transport_kind(&self) -> TransportKind;
}

/// Neutral client TLS profile consumed by transport openers.
pub trait ClientTlsProfile {
    fn server_name(&self) -> Option<&str>;

    fn disable_sni(&self) -> bool;

    fn ca_cert_path(&self) -> Option<&str>;

    fn insecure(&self) -> bool;

    fn alpn(&self) -> &[String];

    fn client_fingerprint(&self) -> Option<&str>;
}

/// Neutral server TLS profile consumed by inbound acceptors.
pub trait ServerTlsProfile {
    fn cert_path(&self) -> &str;

    fn key_path(&self) -> &str;

    fn alpn(&self) -> &[String];

    fn server_fingerprint(&self) -> Option<&str>;
}

/// Neutral WebSocket transport profile consumed by transport openers.
pub trait WebSocketTransportProfile {
    fn path(&self) -> &str;

    fn header_pairs(&self) -> Vec<(String, String)>;
}

/// Neutral gRPC transport profile consumed by transport openers.
pub trait GrpcTransportProfile {
    fn service_names(&self) -> &[String];
}

/// Neutral HTTP/2 transport profile consumed by transport openers.
pub trait H2TransportProfile {
    fn host(&self) -> Option<&str>;

    fn path(&self) -> &str;
}

/// Neutral HTTP upgrade transport profile consumed by transport openers.
pub trait HttpUpgradeTransportProfile {
    fn host(&self) -> Option<&str>;

    fn path(&self) -> &str;
}

/// Neutral SplitHTTP/XHTTP transport profile consumed by transport openers.
pub trait SplitHttpTransportProfile {
    fn host(&self) -> Option<&str>;

    fn path(&self) -> &str;

    fn mode(&self) -> &str;
}

/// Neutral inbound fallback target consumed by runtime fallback replay.
pub trait InboundFallbackProfile {
    fn server(&self) -> &str;

    fn port(&self) -> u16;

    fn alpn(&self) -> Option<&str>;
}

/// Neutral transport identity hints for stream-based protocol MUX profile
/// selection.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct StreamMuxTransportHints {
    tls_server_name: Option<String>,
    ws_path: Option<String>,
    grpc_service_names: Option<Vec<String>>,
    reality_public_key: Option<String>,
    reality_server_name: Option<String>,
    reality_client_fingerprint: Option<String>,
}

impl StreamMuxTransportHints {
    pub fn new(
        tls_server_name: Option<String>,
        ws_path: Option<String>,
        grpc_service_names: Option<Vec<String>>,
        reality_public_key: Option<String>,
        reality_server_name: Option<String>,
    ) -> Self {
        Self {
            tls_server_name,
            ws_path,
            grpc_service_names,
            reality_public_key,
            reality_server_name,
            reality_client_fingerprint: None,
        }
    }

    pub fn with_reality_client_fingerprint(mut self, fingerprint: Option<String>) -> Self {
        self.reality_client_fingerprint = fingerprint;
        self
    }

    pub fn tls_server_name(&self) -> Option<&str> {
        self.tls_server_name.as_deref()
    }

    pub fn ws_path(&self) -> Option<&str> {
        self.ws_path.as_deref()
    }

    pub fn grpc_service_names(&self) -> Option<&[String]> {
        self.grpc_service_names.as_deref()
    }

    pub fn reality_public_key(&self) -> Option<&str> {
        self.reality_public_key.as_deref()
    }

    pub fn reality_server_name(&self) -> Option<&str> {
        self.reality_server_name.as_deref()
    }

    pub fn reality_client_fingerprint(&self) -> Option<&str> {
        self.reality_client_fingerprint.as_deref()
    }
}

// ── Protocol capability descriptors ──────────────────────────────────

/// Support level for a protocol capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolCapabilityLevel {
    Supported,
    Partial,
    Experimental,
    Unsupported,
    NotApplicable,
}

impl ProtocolCapabilityLevel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::Partial => "partial",
            Self::Experimental => "experimental",
            Self::Unsupported => "unsupported",
            Self::NotApplicable => "not_applicable",
        }
    }
}

/// Capability state for one protocol direction and network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolCapabilityState {
    pub supported: bool,
    pub level: ProtocolCapabilityLevel,
    pub notes: &'static [&'static str],
}

impl ProtocolCapabilityState {
    pub const fn supported() -> Self {
        Self {
            supported: true,
            level: ProtocolCapabilityLevel::Supported,
            notes: &[],
        }
    }

    pub const fn partial(notes: &'static [&'static str]) -> Self {
        Self {
            supported: true,
            level: ProtocolCapabilityLevel::Partial,
            notes,
        }
    }

    pub const fn experimental(notes: &'static [&'static str]) -> Self {
        Self {
            supported: true,
            level: ProtocolCapabilityLevel::Experimental,
            notes,
        }
    }

    pub const fn unsupported(notes: &'static [&'static str]) -> Self {
        Self {
            supported: false,
            level: ProtocolCapabilityLevel::Unsupported,
            notes,
        }
    }

    pub const fn not_applicable() -> Self {
        Self {
            supported: false,
            level: ProtocolCapabilityLevel::NotApplicable,
            notes: &[],
        }
    }
}

/// TCP/UDP support for one inbound or outbound direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolNetworkCapability {
    pub tcp: ProtocolCapabilityState,
    pub udp: ProtocolCapabilityState,
}

impl ProtocolNetworkCapability {
    pub const fn new(tcp: ProtocolCapabilityState, udp: ProtocolCapabilityState) -> Self {
        Self { tcp, udp }
    }
}

/// Runtime-neutral protocol capability descriptor.
///
/// This is intentionally not a serde or control-plane type. API adapters map it
/// to their own wire model, while protocol crates can expose the same facts
/// without depending on `zero-api` or the proxy runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolCapabilityDescriptor {
    pub protocol: &'static str,
    pub feature: &'static str,
    pub status: ProtocolCapabilityLevel,
    pub compatibility_baseline: &'static str,
    pub inbound: ProtocolNetworkCapability,
    pub outbound: ProtocolNetworkCapability,
    pub transports: &'static [&'static str],
    pub mux: ProtocolCapabilityState,
    pub limitations: &'static [&'static str],
}

/// Metadata boundary implemented by protocol adapters or protocol crates.
pub trait ProtocolMetadata {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor;
}

/// Protocol TCP tunnel behavior boundary.
///
/// Implementations hide protocol framing and handshake details over an already
/// established upstream stream. Runtime layers remain responsible for dialing,
/// routing, session lifecycle, stats, and events.
pub trait TcpTunnelProtocol<Target: ?Sized>: Send + Sync {
    type Error;

    async fn establish_tcp_tunnel<S>(
        &self,
        stream: &mut S,
        target: &Target,
    ) -> Result<(), Self::Error>
    where
        S: AsyncSocket;
}

/// Protocol TCP tunnel behavior with deferred response validation.
///
/// Some protocols need to write the outbound request immediately but cannot
/// consume the protocol response during establishment because the response
/// must be validated by a stream wrapper on the first downstream read. The
/// runtime still owns dialing, transport setup, metering, lifecycle, stats,
/// and the concrete stream wrapper.
pub trait DeferredTcpTunnelProtocol<Target: ?Sized>: Send + Sync {
    type Error;

    /// Send the protocol request over an already established stream.
    ///
    /// Implementations must not read the protocol response here. The caller is
    /// responsible for wrapping the stream with protocol-specific deferred
    /// response validation before returning it to the relay path.
    async fn send_deferred_tcp_tunnel_request<S>(
        &self,
        stream: &mut S,
        target: &Target,
    ) -> Result<(), Self::Error>
    where
        S: AsyncSocket;
}

/// Protocol UDP relay association behavior boundary.
///
/// Models protocols that establish a UDP relay through a control connection
/// (e.g. SOCKS5 UDP ASSOCIATE). The caller owns the control stream, UDP
/// socket binding, relay address resolution, association caching, idle
/// timeout, stats, events, session lifecycle, and fallback.
///
/// Implementations hide protocol-specific authentication negotiation and
/// UDP relay handshake details over an already established control stream.
pub trait UdpRelayProtocol<Target: ?Sized>: Send + Sync {
    type Error;

    /// The relay endpoint returned by the association handshake.
    /// The caller resolves this into a concrete socket address and binds
    /// a local UDP socket for sending framed packets to this endpoint.
    type RelayEndpoint;

    /// Perform the UDP relay association handshake over an already
    /// established control stream. Returns the relay endpoint that
    /// the caller should send framed packets to.
    async fn establish_udp_relay<S>(
        &self,
        control_stream: &mut S,
        target: &Target,
    ) -> Result<Self::RelayEndpoint, Self::Error>
    where
        S: AsyncSocket;
}

/// Protocol UDP packet tunnel behavior over an established stream.
///
/// Models protocols that carry UDP packets over a connected bidirectional
/// stream. The implementation owns the protocol request/response handshake.
/// The caller owns dialing, transport setup, packet framing after
/// establishment, session lifecycle, stats, events, and fallback behavior.
pub trait UdpPacketTunnelProtocol<Target: ?Sized>: Send + Sync {
    type Error;

    /// Establish the UDP packet tunnel over an already connected stream.
    ///
    /// Implementations should consume any protocol response required before
    /// the stream starts carrying UDP packet payloads.
    async fn establish_udp_packet_tunnel<S>(
        &self,
        stream: &mut S,
        target: &Target,
    ) -> Result<(), Self::Error>
    where
        S: AsyncSocket;
}

/// Protocol UDP packet framing over an established packet tunnel.
///
/// The tunnel caller owns the transport and session lifecycle; protocol crates
/// own how each UDP datagram is encoded into or decoded from tunnel bytes.
pub trait UdpPacketFraming<Packet: ?Sized>: Send + Sync {
    type Error;
    type Decoded;

    fn encode_udp_packet(&self, packet: &Packet) -> Result<Vec<u8>, Self::Error>;

    fn decode_udp_packet(&self, packet: &[u8]) -> Result<Self::Decoded, Self::Error>;
}

/// Protocol UDP packet framing directly on a connected stream.
///
/// This covers protocols whose UDP packet boundary is part of the stream
/// format, for example a length-prefixed packet. The caller still owns
/// dialing, transport setup, caching, lifecycle, stats, and fallback behavior.
pub trait UdpPacketStreamFraming<Packet: ?Sized>: Send + Sync {
    type Error;
    type Decoded;

    async fn write_udp_packet<S>(&self, stream: &mut S, packet: &Packet) -> Result<(), Self::Error>
    where
        S: AsyncSocket;

    async fn read_udp_packet<S>(&self, stream: &mut S) -> Result<Self::Decoded, Self::Error>
    where
        S: AsyncSocket;
}

/// Protocol UDP datagram framing for packet-oriented transports.
///
/// This covers protocols that carry one complete protocol datagram over one
/// UDP datagram. The caller owns sockets, target resolution, caching, routing,
/// lifecycle, stats, events, and fallback behavior. Protocol crates own how a
/// payload is encoded into the wire datagram and decoded back.
pub trait UdpDatagramFraming<Packet: ?Sized, DecodeContext: ?Sized>: Send + Sync {
    type Error;
    type Decoded;

    fn encode_udp_datagram(&self, packet: &Packet) -> Result<Vec<u8>, Self::Error>;

    fn decode_udp_datagram(
        &self,
        context: &DecodeContext,
        datagram: &[u8],
    ) -> Result<Self::Decoded, Self::Error>;
}

/// A packet-oriented transport that carries raw UDP payloads for relay chains.
///
/// Models a carrier that provides send/recv for raw datagrams.
/// Implementations handle their own transport framing (e.g. SOCKS5 UDP
/// header); callers provide and receive plain payloads only.
///
/// Adding new packet path carriers only requires implementing this trait,
/// not creating protocol-pair-specific modules in the proxy runtime.
#[allow(async_fn_in_trait)]
pub trait UdpPacketPath<Target: ?Sized>: Send + Sync + 'static {
    type Error;

    /// Send `payload` to `target:port` through this transport.
    async fn send_to(&self, target: &Target, port: u16, payload: &[u8]) -> Result<(), Self::Error>;

    /// Receive the next datagram, stripping transport framing.
    ///
    /// Returns the number of inner payload bytes written to `buf`.
    async fn recv_from(&self, buf: &mut [u8]) -> Result<usize, Self::Error>;
}

/// Encode/decode UDP datagrams for the inner protocol of a relay chain.
///
/// Each protocol that can be the final hop of a datagram-over-packet-path
/// chain implements this. The codec captures protocol-specific parameters
/// (cipher, password, etc.) so the manager stays protocol-agnostic.
///
/// Adding new inner datagram protocols only requires implementing this trait,
/// not creating protocol-pair-specific modules in the proxy runtime.
pub trait DatagramCodec<Target>: Send + Sync + 'static {
    type Error;

    fn encode(&self, target: &Target, port: u16, payload: &[u8]) -> Result<Vec<u8>, Self::Error>;

    fn decode(&self, data: &[u8]) -> Option<(Target, u16, Vec<u8>)>;
}

/// Protocol TCP outbound behavior that returns session state.
///
/// For protocols whose handshake produces stream or session state (e.g.
/// AEAD encryption context), this trait captures the handshake result as
/// an associated type. The caller owns transport setup, metering, and
/// relay orchestration using the returned session state.
pub trait TcpSessionProtocol<Target: ?Sized>: Send + Sync {
    type Error;
    type Session;

    /// Perform the TCP session handshake over an already established stream.
    /// Returns protocol-specific session state used for subsequent relay.
    async fn establish_tcp_session<S>(
        &self,
        stream: &mut S,
        target: &Target,
    ) -> Result<Self::Session, Self::Error>
    where
        S: AsyncSocket;
}
