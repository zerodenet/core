use core::future::Future;
use core::sync::atomic::{AtomicU8, Ordering};

use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec::Vec;

use zero_traits::{AsyncSocket, SocketAddress};

use crate::{Address, Error, ProtocolType, Session, SessionAuth};

/// Neutral UDP payload routed by proxy/runtime glue.
///
/// Protocol crates convert this shape into protocol-owned packet models before
/// framing or encryption. Runtime glue should prefer this over storing concrete
/// protocol packet structs in manager state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdpFlowPacket {
    pub target: Address,
    pub port: u16,
    pub payload: Vec<u8>,
}

/// Opaque, protocol-supplied identity for reconnecting one logical UDP session.
///
/// The runtime may scope this key by inbound, protocol and authenticated
/// principal, but must not parse its bytes or derive protocol semantics from
/// them. Protocol implementations remain responsible for validating their wire
/// identity before constructing the key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UdpContinuityKey(Vec<u8>);

impl UdpContinuityKey {
    pub const MAX_LEN: usize = 64;

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        (!bytes.is_empty() && bytes.len() <= Self::MAX_LEN).then(|| Self(bytes.to_vec()))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundMuxUdpTermination {
    TransportDetached,
    ExplicitEnd,
}

#[derive(Debug, Clone)]
pub struct InboundMuxUdpTerminationProbe {
    state: Arc<AtomicU8>,
}

impl InboundMuxUdpTerminationProbe {
    pub fn transport_attached() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(0)),
        }
    }

    pub fn mark_explicit_end(&self) {
        self.state.store(1, Ordering::Release);
    }

    pub fn reason(&self) -> InboundMuxUdpTermination {
        if self.state.load(Ordering::Acquire) == 1 {
            InboundMuxUdpTermination::ExplicitEnd
        } else {
            InboundMuxUdpTermination::TransportDetached
        }
    }
}

/// Neutral outbound datagram built by a protocol-owned inbound UDP association.
///
/// Protocol crates use this to hand protocol-framed response payloads plus the
/// destination client endpoint back to proxy/runtime glue. Runtime owns the
/// socket lifecycle and actual datagram send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundUdpAssociationResponse {
    recipient: SocketAddress,
    payload: Vec<u8>,
}

/// Neutral inbound UDP request ready for proxy dispatch.
///
/// Protocol crates convert decoded wire packets into this shape before handing
/// them to proxy runtime glue. The proxy should not need to inspect
/// protocol-specific inbound UDP request structs to route a packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundUdpDispatch {
    target: Address,
    port: u16,
    payload: Vec<u8>,
    protocol: ProtocolType,
    client_session_id: Option<u64>,
    transparent_target: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MuxUdpDecodeFailure {
    Continue,
    End,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundMuxUdpReadFailureAction {
    Continue,
    End,
}

#[derive(Debug)]
pub struct InboundMuxUdpReadFailure {
    pub error: Error,
    pub action: InboundMuxUdpReadFailureAction,
}

/// Protocol-owned inbound MUX UDP relay consumed by shared runtime glue.
///
/// Protocol crates keep MUX payload sources, framing, and response encoding
/// private behind this trait so proxy/runtime code does not unpack
/// protocol-specific relay state just to rebuild a generic request model.
#[async_trait::async_trait]
pub trait InboundMuxUdpRelay: Send {
    async fn read_inbound_dispatch(
        &mut self,
    ) -> Result<Option<InboundUdpDispatch>, InboundMuxUdpReadFailure>;

    fn write_response_for_target(
        &mut self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error>;

    fn end_inbound_stream(&mut self) -> Result<usize, Error>;

    fn mux_session_id(&self) -> u16;

    fn continuity_key(&self) -> Option<&UdpContinuityKey> {
        None
    }

    fn termination_probe(&self) -> Option<InboundMuxUdpTerminationProbe> {
        None
    }

    fn auth(&self) -> Option<&SessionAuth> {
        None
    }
}

/// Protocol-owned inbound MUX TCP relay consumed by shared runtime glue.
pub trait InboundMuxTcpRelay: Send + 'static {
    fn mux_session_id(&self) -> u16;

    fn close_stream(&self) -> impl Future<Output = ()> + Send;

    fn relay_stream<S>(self, upstream: S) -> impl Future<Output = ()> + Send
    where
        S: AsyncSocket + 'static,
        S::Error: Send;
}

#[async_trait::async_trait]
pub trait InboundMuxServer<R>: Send {
    type TcpRelay: InboundMuxTcpRelay;
    type UdpRelay: InboundMuxUdpRelay;

    fn auth(&self) -> Option<&SessionAuth> {
        None
    }

    async fn dispatch_next_opened_route<E, FTcp, FUdp>(
        &mut self,
        reader: &mut R,
        on_tcp_opened: FTcp,
        on_udp_opened: FUdp,
    ) -> Result<bool, E>
    where
        E: From<Error>,
        FTcp: FnOnce(Session, Self::TcpRelay) -> Result<(), E> + Send,
        FUdp: FnOnce(Self::UdpRelay) -> Result<(), E> + Send;
}

pub trait MuxUdpResponder: Send {
    fn decode_inbound_dispatch(&mut self, payload: &[u8]) -> Result<InboundUdpDispatch, Error>;

    fn write_response_for_target(
        &mut self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error>;

    fn end_inbound_stream(&mut self) -> Result<usize, Error>;

    fn decode_failure(&self) -> MuxUdpDecodeFailure {
        MuxUdpDecodeFailure::End
    }
}

#[async_trait::async_trait]
pub trait StreamUdpResponder<S>: Send
where
    S: Send,
{
    async fn read_inbound_dispatch(
        &mut self,
        client: &mut S,
    ) -> Result<Option<InboundUdpDispatch>, Error>;

    async fn write_response_for_target(
        &mut self,
        client: &mut S,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error>;
}

pub trait InboundStreamUdpRelay: Send {
    type Stream: Send;
    type Responder: StreamUdpResponder<Self::Stream>;

    fn into_stream_udp_parts(self) -> (Self::Stream, Self::Responder, Option<SessionAuth>);
}

pub trait InboundDatagramUdpRelay<S>: Send
where
    S: Send,
{
    type Responder: DatagramUdpResponder<S>;

    fn into_datagram_udp_parts(self) -> (Self::Responder, Option<SessionAuth>);
}

#[async_trait::async_trait]
pub trait InboundStreamRoute: Send {
    type TcpStream;
    type UdpRelay: InboundStreamUdpRelay;

    async fn dispatch_inbound_route<E, FTcp, FTcpFut, FUdp, FUdpFut>(
        self,
        on_tcp: FTcp,
        on_udp: FUdp,
    ) -> Result<(), E>
    where
        FTcp: FnOnce(Session, Self::TcpStream) -> FTcpFut + Send,
        FTcpFut: Future<Output = Result<(), E>> + Send,
        FUdp: FnOnce(Session, Self::UdpRelay) -> FUdpFut + Send,
        FUdpFut: Future<Output = Result<(), E>> + Send;
}

#[async_trait::async_trait]
pub trait InboundMuxStreamRoute: Send {
    type TcpStream;
    type UdpRelay: InboundStreamUdpRelay;
    type MuxReader;
    type MuxServer: Send;

    async fn dispatch_inbound_route<E, FTcp, FTcpFut, FUdp, FUdpFut, FMux, FMuxFut>(
        self,
        on_tcp: FTcp,
        on_udp: FUdp,
        on_mux: FMux,
    ) -> Result<(), E>
    where
        FTcp: FnOnce(Session, Self::TcpStream) -> FTcpFut + Send,
        FTcpFut: Future<Output = Result<(), E>> + Send,
        FUdp: FnOnce(Session, Self::UdpRelay) -> FUdpFut + Send,
        FUdpFut: Future<Output = Result<(), E>> + Send,
        FMux: FnOnce(Self::MuxReader, Self::MuxServer) -> FMuxFut + Send,
        FMuxFut: Future<Output = Result<(), E>> + Send;
}

#[async_trait::async_trait]
pub trait DatagramUdpResponder<S>: Send
where
    S: Send,
{
    async fn read_inbound_dispatch(
        &mut self,
        source: &S,
    ) -> Result<Option<InboundUdpDispatch>, Error>;

    fn auth(&self) -> Option<&SessionAuth> {
        None
    }

    fn on_dispatch_success(&mut self, _session_id: u64, _dispatch: &InboundUdpDispatch) {}

    async fn write_response_for_session(
        &mut self,
        source: &S,
        session_id: Option<u64>,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Option<usize>, Error>;
}

#[allow(async_fn_in_trait)]
pub trait InboundUdpAssociationDispatcher {
    type Error;

    async fn dispatch_local_dns(&mut self, domain: &str) -> Result<(), Self::Error>;

    async fn dispatch_inbound_packet(
        &mut self,
        dispatch: InboundUdpDispatch,
        protocol_overhead_bytes: u64,
    ) -> Result<(), Self::Error>;

    async fn dispatch_peer_response(
        &mut self,
        sender: SocketAddress,
        payload: &[u8],
    ) -> Result<(), Self::Error>;

    async fn dispatch_unexpected_sender(
        &mut self,
        sender: SocketAddress,
    ) -> Result<(), Self::Error>;
}

#[allow(async_fn_in_trait)]
pub trait InboundUdpAssociation: Send {
    async fn dispatch_datagram<D>(
        &mut self,
        sender: SocketAddress,
        packet: &[u8],
        dispatcher: &mut D,
    ) -> Result<(), D::Error>
    where
        D: InboundUdpAssociationDispatcher,
        D::Error: From<Error>;
}

pub trait InboundUdpAssociationResponder: Send {
    fn build_response_for_target(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Option<InboundUdpAssociationResponse>, Error>;

    fn build_peer_response(
        &self,
        sender: SocketAddress,
        payload: &[u8],
    ) -> Result<Option<InboundUdpAssociationResponse>, Error>;
}

impl InboundUdpDispatch {
    pub fn new(
        protocol: ProtocolType,
        target: Address,
        port: u16,
        payload: Vec<u8>,
        client_session_id: Option<u64>,
    ) -> Self {
        Self {
            target,
            port,
            payload,
            protocol,
            client_session_id,
            transparent_target: false,
        }
    }

    /// Mark this packet as originating from transparent interception.
    pub fn with_transparent_target(mut self) -> Self {
        self.transparent_target = true;
        self
    }

    pub fn protocol(&self) -> ProtocolType {
        self.protocol
    }

    pub fn target(&self) -> &Address {
        &self.target
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn client_session_id(&self) -> Option<u64> {
        self.client_session_id
    }

    pub fn transparent_target(&self) -> bool {
        self.transparent_target
    }

    pub fn into_parts(self) -> (ProtocolType, Address, u16, Vec<u8>, Option<u64>) {
        (
            self.protocol,
            self.target,
            self.port,
            self.payload,
            self.client_session_id,
        )
    }
}

impl InboundUdpAssociationResponse {
    pub fn new(recipient: SocketAddress, payload: Vec<u8>) -> Self {
        Self { recipient, payload }
    }

    pub fn recipient(&self) -> SocketAddress {
        self.recipient
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn into_parts(self) -> (SocketAddress, Vec<u8>) {
        (self.recipient, self.payload)
    }
}

impl UdpFlowPacket {
    pub fn new(target: Address, port: u16, payload: Vec<u8>) -> Self {
        Self {
            target,
            port,
            payload,
        }
    }

    pub fn from_parts(target: &Address, port: u16, payload: &[u8]) -> Self {
        Self::new(target.clone(), port, payload.to_vec())
    }

    pub fn into_parts(self) -> (Address, u16, Vec<u8>) {
        (self.target, self.port, self.payload)
    }
}
