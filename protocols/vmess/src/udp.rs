use core::future::Future;
use tokio::sync::{broadcast, mpsc, oneshot};
use zero_core::{
    Address, Error, InboundUdpDispatch, MuxUdpResponder, Network, ProtocolType, Session,
    StreamUdpResponder,
};
use zero_traits::{AsyncSocket, UdpPacketFraming, UdpPacketTunnelProtocol};

use crate::outbound::VmessOutbound;
use crate::shared::{
    establish_outbound_session, parse_address_from_bytes, write_address, VmessOutboundSession,
    CMD_UDP,
};
use crate::stream::VmessAeadStream;
use crate::{parse_uuid, VmessCipher};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VmessUdpPayloadMode {
    VmessPacket,
    RawDatagram,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VmessUdpPayloadState {
    Unknown,
    Mode(VmessUdpPayloadMode),
}

/// Target parameters for a VMess UDP packet tunnel over a connected stream.
#[derive(Debug, Clone, Copy)]
struct VmessUdpPacketTunnelTarget<'a> {
    pub session: &'a Session,
    pub uuid: &'a [u8; 16],
    pub cipher: VmessCipher,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VmessUdpIdentity {
    pub uuid: [u8; 16],
    pub cipher: VmessCipher,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VmessUdpFlowConfig<'a> {
    identity: VmessUdpIdentity,
    cipher_name: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VmessUdpFlowResume {
    identity: VmessUdpIdentity,
    cipher_name: String,
    relay_chain: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VmessUdpFlowPlan {
    resume: VmessUdpFlowResume,
    mux_concurrency: Option<u32>,
    mux_idle_timeout_secs: Option<u64>,
    mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedVmessUdpFlowPlan {
    plan: VmessUdpFlowPlan,
    mux_transport_profile: crate::mux::OwnedVmessMuxTransportProfile,
}

impl VmessUdpFlowPlan {
    fn new(
        resume: VmessUdpFlowResume,
        mux_concurrency: Option<u32>,
        mux_idle_timeout_secs: Option<u64>,
        mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
    ) -> Self {
        Self {
            resume,
            mux_concurrency,
            mux_idle_timeout_secs,
            mux_response_backlog,
        }
    }

    pub(crate) fn direct_from_config(
        id: &str,
        cipher: &str,
        mux_concurrency: Option<u32>,
        mux_idle_timeout_secs: Option<u64>,
        mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
    ) -> Result<Self, Error> {
        udp_direct_flow_resume_from_config(id, cipher).map(|resume| {
            VmessUdpFlowPlan::new(
                resume,
                mux_concurrency,
                mux_idle_timeout_secs,
                mux_response_backlog,
            )
        })
    }

    pub(crate) fn relay_from_config(id: &str, cipher: &str) -> Result<Self, Error> {
        udp_relay_flow_resume_from_config(id, cipher).map(|resume| {
            VmessUdpFlowPlan::new(
                resume,
                None,
                None,
                crate::mux::MuxResponseBacklogPolicy::default(),
            )
        })
    }

    pub fn connector_flow(
        &self,
        server: &str,
        port: u16,
        session_id: u64,
    ) -> VmessUdpConnectorFlow {
        self.resume().connector_flow(server, port, session_id)
    }

    fn resume(&self) -> &VmessUdpFlowResume {
        &self.resume
    }

    fn mux_concurrency(&self) -> Option<u32> {
        self.mux_concurrency
    }

    pub(crate) async fn open_udp_flow_with_transport_or_mux<S, OpenStream, OpenStreamFut, E>(
        &self,
        session: &Session,
        server: &str,
        port: u16,
        profile: crate::mux::VmessMuxTransportProfile<'_>,
        mux_pool: &crate::mux::VmessMuxConnectionPool,
        open_stream: OpenStream,
    ) -> Result<VmessUdpFlowConnection, E>
    where
        S: AsyncSocket
            + tokio::io::AsyncRead
            + tokio::io::AsyncWrite
            + Unpin
            + Send
            + Sync
            + 'static,
        OpenStream: FnOnce() -> OpenStreamFut,
        OpenStreamFut: Future<Output = Result<S, E>>,
        E: From<Error>,
    {
        let resume = self.resume();
        if let Some(max_concurrency) = self.mux_concurrency() {
            let key = resume
                .udp_mux_pool_key_from_transport_config(
                    server,
                    port,
                    profile,
                    self.mux_idle_timeout_secs,
                    self.mux_response_backlog,
                )
                .map_err(E::from)?;
            let stream = mux_pool
                .open_udp_stream(
                    key,
                    max_concurrency,
                    session.target.clone(),
                    session.port,
                    open_stream,
                )
                .await?;
            return Ok(start_udp_flow(stream));
        }

        let stream = open_stream().await?;
        establish_udp_flow_with_resume(stream, session, resume)
            .await
            .map_err(E::from)
    }

    pub(crate) async fn open_relay_udp_flow_with_transport<S, OpenTransport, OpenTransportFut, E>(
        &self,
        session: &Session,
        stream: S,
        open_transport: OpenTransport,
    ) -> Result<VmessUdpFlowConnection, E>
    where
        S: AsyncSocket
            + tokio::io::AsyncRead
            + tokio::io::AsyncWrite
            + Unpin
            + Send
            + Sync
            + 'static,
        OpenTransport: FnOnce(S) -> OpenTransportFut,
        OpenTransportFut: Future<Output = Result<S, E>>,
        E: From<Error>,
    {
        let stream = open_transport(stream).await?;
        establish_udp_flow_with_resume(stream, session, self.resume())
            .await
            .map_err(E::from)
    }
}

impl PreparedVmessUdpFlowPlan {
    pub(crate) fn new(
        plan: VmessUdpFlowPlan,
        mux_transport_profile: crate::mux::OwnedVmessMuxTransportProfile,
    ) -> Self {
        Self {
            plan,
            mux_transport_profile,
        }
    }

    pub fn connector_flow(
        &self,
        server: &str,
        port: u16,
        session_id: u64,
    ) -> VmessUdpConnectorFlow {
        self.plan.connector_flow(server, port, session_id)
    }

    pub async fn open_udp_flow_with_transport_or_mux<S, OpenStream, OpenStreamFut, E>(
        &self,
        session: &Session,
        server: &str,
        port: u16,
        mux_pool: &crate::mux::VmessMuxConnectionPool,
        open_stream: OpenStream,
    ) -> Result<VmessUdpFlowConnection, E>
    where
        S: AsyncSocket
            + tokio::io::AsyncRead
            + tokio::io::AsyncWrite
            + Unpin
            + Send
            + Sync
            + 'static,
        OpenStream: FnOnce() -> OpenStreamFut,
        OpenStreamFut: Future<Output = Result<S, E>>,
        E: From<Error>,
    {
        self.plan
            .open_udp_flow_with_transport_or_mux(
                session,
                server,
                port,
                self.mux_transport_profile.as_borrowed(),
                mux_pool,
                open_stream,
            )
            .await
    }

    pub async fn open_relay_udp_flow_with_transport<S, OpenTransport, OpenTransportFut, E>(
        &self,
        session: &Session,
        stream: S,
        open_transport: OpenTransport,
    ) -> Result<VmessUdpFlowConnection, E>
    where
        S: AsyncSocket
            + tokio::io::AsyncRead
            + tokio::io::AsyncWrite
            + Unpin
            + Send
            + Sync
            + 'static,
        OpenTransport: FnOnce(S) -> OpenTransportFut,
        OpenTransportFut: Future<Output = Result<S, E>>,
        E: From<Error>,
    {
        self.plan
            .open_relay_udp_flow_with_transport(session, stream, open_transport)
            .await
    }
}

impl VmessUdpFlowResume {
    fn identity(&self) -> VmessUdpIdentity {
        self.identity
    }

    fn mux_pool_identity(&self) -> crate::mux::VmessMuxIdentity {
        crate::mux::VmessMuxIdentity::from_parts(
            self.identity.uuid,
            self.cipher_name.clone(),
            self.identity.cipher,
        )
    }

    fn udp_mux_pool_key_from_transport_config(
        &self,
        server: &str,
        port: u16,
        profile: crate::mux::VmessMuxTransportProfile<'_>,
        mux_idle_timeout_secs: Option<u64>,
        mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
    ) -> Result<crate::mux::VmessMuxPoolKey, Error> {
        crate::mux::pool_key_from_transport_config(
            server,
            port,
            self.mux_pool_identity(),
            profile,
            mux_idle_timeout_secs,
            mux_response_backlog,
        )
    }

    fn flow_requires_relay_upstream(&self) -> bool {
        self.relay_chain
    }

    fn connector_flow(&self, server: &str, port: u16, session_id: u64) -> VmessUdpConnectorFlow {
        VmessUdpConnectorFlow {
            cache_key: format!(
                "vmess:{server}:{port}:{session_id}:relay={}",
                self.relay_chain
            ),
            requires_relay_upstream: self.flow_requires_relay_upstream(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VmessUdpConnectorFlow {
    cache_key: String,
    requires_relay_upstream: bool,
}

impl VmessUdpConnectorFlow {
    pub fn into_parts(self) -> (String, bool) {
        (self.cache_key, self.requires_relay_upstream)
    }
}

impl<'a> VmessUdpFlowConfig<'a> {
    fn new(id: &str, cipher: &'a str) -> Result<Self, Error> {
        Ok(Self {
            identity: parse_udp_identity(id, cipher)?,
            cipher_name: cipher,
        })
    }

    fn flow_resume(&self, relay_chain: bool) -> VmessUdpFlowResume {
        VmessUdpFlowResume {
            identity: self.identity,
            cipher_name: self.cipher_name.into(),
            relay_chain,
        }
    }
}

fn udp_flow_resume_from_config(
    id: &str,
    cipher: &str,
    relay_chain: bool,
) -> Result<VmessUdpFlowResume, Error> {
    VmessUdpFlowConfig::new(id, cipher).map(|config| config.flow_resume(relay_chain))
}

fn udp_direct_flow_resume_from_config(id: &str, cipher: &str) -> Result<VmessUdpFlowResume, Error> {
    udp_flow_resume_from_config(id, cipher, false)
}

fn udp_relay_flow_resume_from_config(id: &str, cipher: &str) -> Result<VmessUdpFlowResume, Error> {
    udp_flow_resume_from_config(id, cipher, true)
}

fn parse_udp_identity(id: &str, cipher: &str) -> Result<VmessUdpIdentity, Error> {
    let uuid = parse_uuid(id)?;
    let cipher = VmessCipher::from_name(cipher).ok_or(Error::Protocol("vmess unknown cipher"))?;
    Ok(VmessUdpIdentity { uuid, cipher })
}

/// One UDP datagram to encode for a VMess UDP packet tunnel.
#[derive(Debug, Clone, Copy)]
pub struct VmessUdpPacketTarget<'a> {
    pub address: &'a Address,
    pub port: u16,
    pub payload: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VmessUdpPacket {
    target: Address,
    port: u16,
    payload: Vec<u8>,
}

impl VmessUdpPacket {
    pub fn new(target: Address, port: u16, payload: Vec<u8>) -> Self {
        Self {
            target,
            port,
            payload,
        }
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

    pub fn into_parts(self) -> (Address, u16, Vec<u8>) {
        (self.target, self.port, self.payload)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VmessUdpFlowPacket {
    target: Address,
    port: u16,
    payload: Vec<u8>,
}

impl VmessUdpFlowPacket {
    pub fn new(target: Address, port: u16, payload: Vec<u8>) -> Self {
        Self {
            target,
            port,
            payload,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        encode_udp_flow_packet(&self.target, self.port, &self.payload)
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

    pub fn into_parts(self) -> (Address, u16, Vec<u8>) {
        (self.target, self.port, self.payload)
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct VmessUdpFlowIo;

impl VmessUdpFlowIo {
    fn encode_packet(&self, target: &Address, port: u16, payload: &[u8]) -> Result<Vec<u8>, Error> {
        encode_udp_flow_packet(target, port, payload)
    }

    fn decode_packet(&self, packet: &[u8]) -> Result<VmessUdpFlowPacket, Error> {
        let packet = decode_udp_flow_packet(packet)?;
        let (target, port, payload) = packet.into_parts();
        Ok(VmessUdpFlowPacket::new(target, port, payload))
    }

    async fn write_packet_tokio<S>(
        &self,
        stream: &mut S,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error>
    where
        S: tokio::io::AsyncWrite + Unpin,
    {
        let encoded = self.encode_packet(target, port, payload)?;
        let len = encoded.len();
        tokio::io::AsyncWriteExt::write_all(stream, &encoded)
            .await
            .map_err(|_| Error::Io("vmess udp flow write"))?;
        tokio::io::AsyncWriteExt::flush(stream)
            .await
            .map_err(|_| Error::Io("vmess udp flow flush"))?;
        Ok(len)
    }

    async fn read_packet_tokio<S>(
        &self,
        stream: &mut S,
        buffer: &mut [u8],
    ) -> Result<Option<VmessUdpFlowPacket>, Error>
    where
        S: tokio::io::AsyncRead + Unpin,
    {
        let n = tokio::io::AsyncReadExt::read(stream, buffer)
            .await
            .map_err(|_| Error::Io("vmess udp flow read"))?;
        if n == 0 {
            return Ok(None);
        }
        self.decode_packet(&buffer[..n]).map(Some)
    }
}

pub struct VmessInboundUdpPayload {
    state: VmessUdpPayloadState,
    target: Address,
    port: u16,
    payload: Vec<u8>,
}

impl VmessInboundUdpPayload {
    fn new(state: VmessUdpPayloadState, target: Address, port: u16, payload: Vec<u8>) -> Self {
        Self {
            state,
            target,
            port,
            payload,
        }
    }

    pub fn state(&self) -> VmessUdpPayloadState {
        self.state
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

    fn into_parts(self) -> (VmessUdpPayloadState, Address, u16, Vec<u8>) {
        (self.state, self.target, self.port, self.payload)
    }
}

/// Protocol-owned decoded inbound UDP request.
///
/// Proxy inbound glue submits this native datagram request to its UDP pipe
/// without depending on VMess wire payload state or packet fields.
struct VmessInboundUdpRequest {
    target: Address,
    port: u16,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VmessInboundUdpDispatchParts {
    target: Address,
    port: u16,
    payload: Vec<u8>,
    client_session_id: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct VmessInboundUdpClientResponse<'a> {
    target: &'a Address,
    port: u16,
    payload: &'a [u8],
}

impl<'a> VmessInboundUdpClientResponse<'a> {
    pub fn new(target: &'a Address, port: u16, payload: &'a [u8]) -> Self {
        Self {
            target,
            port,
            payload,
        }
    }

    fn target(&self) -> &'a Address {
        self.target
    }

    fn port(&self) -> u16 {
        self.port
    }

    fn payload(&self) -> &'a [u8] {
        self.payload
    }
}

impl VmessInboundUdpRequest {
    fn from_payload(payload: VmessInboundUdpPayload) -> (Self, VmessUdpPayloadState) {
        let (state, target, port, payload) = payload.into_parts();
        (
            Self {
                target,
                port,
                payload,
            },
            state,
        )
    }

    pub fn into_parts(self) -> (Address, u16, Vec<u8>) {
        (self.target, self.port, self.payload)
    }

    pub fn into_dispatch_parts(self) -> VmessInboundUdpDispatchParts {
        let (target, port, payload) = self.into_parts();
        VmessInboundUdpDispatchParts {
            target,
            port,
            payload,
            client_session_id: None,
        }
    }
}

impl VmessInboundUdpDispatchParts {
    pub fn protocol(&self) -> ProtocolType {
        ProtocolType::new("vmess")
    }

    pub fn into_parts(self) -> (Address, u16, Vec<u8>, Option<u64>) {
        (self.target, self.port, self.payload, self.client_session_id)
    }

    pub fn into_inbound_dispatch(self) -> InboundUdpDispatch {
        let protocol = self.protocol();
        let (target, port, payload, client_session_id) = self.into_parts();
        InboundUdpDispatch::new(protocol, target, port, payload, client_session_id)
    }
}

/// Stateful inbound UDP codec wrapper for VMess packet/raw payload detection.
#[derive(Debug, Clone)]
pub struct VmessInboundUdpSession {
    state: VmessUdpPayloadState,
    default_target: Address,
    default_port: u16,
}

pub struct VmessInboundUdpResponder {
    session: VmessInboundUdpSession,
    read_buf: Vec<u8>,
}

pub(crate) struct VmessInboundMuxUdpResponder {
    session: VmessInboundUdpSession,
    writer: crate::mux::VmessInboundMuxWriter,
    mux_session_id: u16,
}

impl VmessInboundUdpSession {
    pub fn new(default_target: Address, default_port: u16) -> Self {
        Self {
            state: VmessUdpPayloadState::Unknown,
            default_target,
            default_port,
        }
    }

    fn decode_request(&mut self, payload: &[u8]) -> Result<VmessInboundUdpRequest, Error> {
        let decoded = VmessInboundUdpCodec.decode_datagram(
            self.state,
            &self.default_target,
            self.default_port,
            payload,
        )?;
        let (request, state) = VmessInboundUdpRequest::from_payload(decoded);
        self.state = state;
        Ok(request)
    }

    fn decode_dispatch_parts(
        &mut self,
        payload: &[u8],
    ) -> Result<VmessInboundUdpDispatchParts, Error> {
        self.decode_request(payload)
            .map(VmessInboundUdpRequest::into_dispatch_parts)
    }

    pub fn decode_inbound_dispatch(&mut self, payload: &[u8]) -> Result<InboundUdpDispatch, Error> {
        self.decode_dispatch_parts(payload)
            .map(VmessInboundUdpDispatchParts::into_inbound_dispatch)
    }

    pub(crate) fn decode_mux_inbound_dispatch(
        &mut self,
        payload: &[u8],
    ) -> Result<InboundUdpDispatch, Error> {
        self.decode_inbound_dispatch(payload)
    }

    async fn read_dispatch_parts_tokio<R>(
        &mut self,
        reader: &mut R,
        buf: &mut [u8],
    ) -> Result<Option<VmessInboundUdpDispatchParts>, Error>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        let n = tokio::io::AsyncReadExt::read(reader, buf)
            .await
            .map_err(|_| Error::Io("failed to read VMess UDP request"))?;
        if n == 0 {
            return Ok(None);
        }
        self.decode_dispatch_parts(&buf[..n]).map(Some)
    }

    pub(crate) async fn read_inbound_dispatch_tokio<R>(
        &mut self,
        reader: &mut R,
        buf: &mut [u8],
    ) -> Result<Option<InboundUdpDispatch>, Error>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        self.read_dispatch_parts_tokio(reader, buf)
            .await
            .map(|parts| parts.map(VmessInboundUdpDispatchParts::into_inbound_dispatch))
    }

    pub async fn write_response_tokio<W>(
        &self,
        writer: &mut W,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        VmessInboundUdpCodec
            .write_response_tokio(writer, self.state, target, port, payload)
            .await
    }

    async fn write_client_response_tokio<W>(
        &self,
        writer: &mut W,
        response: VmessInboundUdpClientResponse<'_>,
    ) -> Result<usize, Error>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        self.write_response_tokio(
            writer,
            response.target(),
            response.port(),
            response.payload(),
        )
        .await
    }

    async fn write_client_response_for_target_tokio<W>(
        &self,
        writer: &mut W,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        self.write_client_response_tokio(
            writer,
            VmessInboundUdpClientResponse::new(target, port, payload),
        )
        .await
    }

    pub(crate) fn write_mux_response(
        &self,
        writer: &crate::mux::VmessInboundMuxWriter,
        mux_session_id: u16,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error> {
        let frame = VmessInboundUdpCodec.encode_mux_response_for_state(
            mux_session_id,
            self.state,
            target,
            port,
            payload,
        )?;
        writer.frame(frame)
    }

    pub fn encode_mux_response_packet(
        &self,
        mux_session_id: u16,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Vec<u8>, Error> {
        VmessInboundUdpCodec.encode_mux_response_for_state(
            mux_session_id,
            self.state,
            target,
            port,
            payload,
        )
    }

    fn write_mux_client_response(
        &self,
        writer: &crate::mux::VmessInboundMuxWriter,
        mux_session_id: u16,
        response: VmessInboundUdpClientResponse<'_>,
    ) -> Result<usize, Error> {
        self.write_mux_response(
            writer,
            mux_session_id,
            response.target(),
            response.port(),
            response.payload(),
        )
    }

    fn write_mux_client_response_for_target(
        &self,
        writer: &crate::mux::VmessInboundMuxWriter,
        mux_session_id: u16,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error> {
        self.write_mux_client_response(
            writer,
            mux_session_id,
            VmessInboundUdpClientResponse::new(target, port, payload),
        )
    }
}

impl VmessInboundUdpResponder {
    pub(crate) fn new(session: VmessInboundUdpSession) -> Self {
        Self {
            session,
            read_buf: vec![0_u8; 64 * 1024],
        }
    }

    async fn read_inbound_dispatch_tokio<R>(
        &mut self,
        reader: &mut R,
    ) -> Result<Option<InboundUdpDispatch>, Error>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        self.session
            .read_inbound_dispatch_tokio(reader, &mut self.read_buf)
            .await
    }

    async fn write_response_for_target_tokio<W>(
        &self,
        writer: &mut W,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        self.session
            .write_client_response_for_target_tokio(writer, target, port, payload)
            .await
    }
}

#[async_trait::async_trait]
impl<S> StreamUdpResponder<S> for VmessInboundUdpResponder
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin,
{
    async fn read_inbound_dispatch(
        &mut self,
        client: &mut S,
    ) -> Result<Option<InboundUdpDispatch>, Error> {
        self.read_inbound_dispatch_tokio(client).await
    }

    async fn write_response_for_target(
        &mut self,
        client: &mut S,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error> {
        self.write_response_for_target_tokio(client, target, port, payload)
            .await
    }
}

impl VmessInboundMuxUdpResponder {
    pub(crate) fn new(
        session: VmessInboundUdpSession,
        writer: crate::mux::VmessInboundMuxWriter,
        mux_session_id: u16,
    ) -> Self {
        Self {
            session,
            writer,
            mux_session_id,
        }
    }

    pub(crate) fn decode_inbound_dispatch(
        &mut self,
        payload: &[u8],
    ) -> Result<InboundUdpDispatch, Error> {
        self.session.decode_mux_inbound_dispatch(payload)
    }

    pub(crate) fn write_response_for_target(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error> {
        self.session.write_mux_client_response_for_target(
            &self.writer,
            self.mux_session_id,
            target,
            port,
            payload,
        )
    }

    pub(crate) fn end_inbound_stream(&self) -> Result<usize, Error> {
        self.writer.end_inbound_stream(self.mux_session_id)
    }
}

impl MuxUdpResponder for VmessInboundMuxUdpResponder {
    fn decode_inbound_dispatch(&mut self, payload: &[u8]) -> Result<InboundUdpDispatch, Error> {
        VmessInboundMuxUdpResponder::decode_inbound_dispatch(self, payload)
    }

    fn write_response_for_target(
        &mut self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error> {
        VmessInboundMuxUdpResponder::write_response_for_target(self, target, port, payload)
    }

    fn end_inbound_stream(&mut self) -> Result<usize, Error> {
        VmessInboundMuxUdpResponder::end_inbound_stream(self)
    }
}

impl VmessOutbound {
    pub async fn establish_udp_packet_session<S>(
        &self,
        stream: &mut S,
        session: &Session,
        uuid: &[u8; 16],
        cipher: VmessCipher,
    ) -> Result<VmessOutboundSession, Error>
    where
        S: AsyncSocket,
    {
        let udp_session = Session::new(
            session.id,
            session.target.clone(),
            session.port,
            Network::Udp,
            ProtocolType::new("vmess"),
        );
        establish_outbound_session(stream, &udp_session, uuid, cipher, CMD_UDP).await
    }
}

impl<'a> UdpPacketTunnelProtocol<VmessUdpPacketTunnelTarget<'a>> for VmessOutbound {
    type Error = Error;

    async fn establish_udp_packet_tunnel<S>(
        &self,
        stream: &mut S,
        target: &VmessUdpPacketTunnelTarget<'a>,
    ) -> Result<(), Self::Error>
    where
        S: AsyncSocket,
    {
        self.establish_udp_packet_session(stream, target.session, target.uuid, target.cipher)
            .await
            .map(|_| ())
    }
}

impl<'a> UdpPacketFraming<VmessUdpPacketTarget<'a>> for VmessOutbound {
    type Error = Error;
    type Decoded = VmessUdpPacket;

    fn encode_udp_packet(&self, packet: &VmessUdpPacketTarget<'a>) -> Result<Vec<u8>, Self::Error> {
        build_udp_packet(packet.address, packet.port, packet.payload)
    }

    fn decode_udp_packet(&self, packet: &[u8]) -> Result<Self::Decoded, Self::Error> {
        parse_udp_packet(packet)
    }
}

impl<S> VmessAeadStream<S> {
    pub async fn establish_udp_outbound(
        mut inner: S,
        outbound: &VmessOutbound,
        session: &Session,
        uuid: &[u8; 16],
        cipher: VmessCipher,
    ) -> Result<Self, Error>
    where
        S: AsyncSocket,
    {
        let vmess_session = outbound
            .establish_udp_packet_session(&mut inner, session, uuid, cipher)
            .await?;
        VmessAeadStream::outbound(inner, vmess_session)
    }
}

pub async fn establish_udp_outbound_stream<S>(
    stream: S,
    session: &Session,
    uuid: &[u8; 16],
    cipher: VmessCipher,
) -> Result<VmessAeadStream<S>, Error>
where
    S: AsyncSocket,
{
    VmessAeadStream::establish_udp_outbound(stream, &VmessOutbound, session, uuid, cipher).await
}

async fn establish_udp_flow_stream<S>(
    stream: S,
    session: &Session,
    identity: VmessUdpIdentity,
) -> Result<VmessAeadStream<S>, Error>
where
    S: AsyncSocket,
{
    establish_udp_outbound_stream(stream, session, &identity.uuid, identity.cipher).await
}

#[derive(Debug, Clone, Copy, Default)]
struct VmessEstablishedUdpFlow {
    io: VmessUdpFlowIo,
}

pub type VmessUdpFlowResponse = (Address, u16, Vec<u8>);

type VmessUdpFlowResponses = broadcast::Sender<VmessUdpFlowResponse>;

pub type VmessUdpFlowResponseReceiver = broadcast::Receiver<VmessUdpFlowResponse>;

struct VmessUdpFlowSend {
    packet: zero_core::UdpFlowPacket,
    result_tx: oneshot::Sender<Result<usize, Error>>,
}

#[derive(Clone)]
struct VmessUdpFlowSender {
    send_tx: mpsc::Sender<VmessUdpFlowSend>,
}

struct VmessUdpFlowHandle {
    sender: VmessUdpFlowSender,
    responses: VmessUdpFlowResponses,
}

#[derive(Clone)]
struct VmessUdpFlowSession {
    sender: VmessUdpFlowSender,
    responses: VmessUdpFlowResponses,
}

impl VmessUdpFlowSession {
    fn new(handle: VmessUdpFlowHandle) -> Self {
        Self {
            sender: handle.sender,
            responses: handle.responses,
        }
    }

    async fn send(&self, target: &Address, port: u16, payload: &[u8]) -> Result<usize, Error> {
        self.sender.send(target, port, payload).await
    }

    fn subscribe_responses(&self) -> VmessUdpFlowResponseReceiver {
        self.responses.subscribe()
    }
}

#[derive(Clone)]
pub struct VmessUdpFlowConnection {
    session: VmessUdpFlowSession,
}

impl VmessUdpFlowConnection {
    fn new(handle: VmessUdpFlowHandle) -> Self {
        Self {
            session: VmessUdpFlowSession::new(handle),
        }
    }

    pub async fn send(&self, target: &Address, port: u16, payload: &[u8]) -> Result<usize, Error> {
        self.session.send(target, port, payload).await
    }

    pub fn subscribe_responses(&self) -> VmessUdpFlowResponseReceiver {
        self.session.subscribe_responses()
    }
}

impl VmessUdpFlowSender {
    async fn send(&self, target: &Address, port: u16, payload: &[u8]) -> Result<usize, Error> {
        let packet = zero_core::UdpFlowPacket::from_parts(target, port, payload);
        let (result_tx, result_rx) = oneshot::channel();
        self.send_tx
            .send(VmessUdpFlowSend { packet, result_tx })
            .await
            .map_err(|_| Error::Io("vmess udp flow closed"))?;
        result_rx
            .await
            .map_err(|_| Error::Io("vmess udp flow closed"))?
    }
}

impl VmessEstablishedUdpFlow {
    async fn write_packet_tokio<S>(
        &self,
        stream: &mut S,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error>
    where
        S: tokio::io::AsyncWrite + Unpin,
    {
        self.io
            .write_packet_tokio(stream, target, port, payload)
            .await
    }

    async fn read_packet_tokio<S>(
        &self,
        stream: &mut S,
        buffer: &mut [u8],
    ) -> Result<Option<VmessUdpFlowPacket>, Error>
    where
        S: tokio::io::AsyncRead + Unpin,
    {
        self.io.read_packet_tokio(stream, buffer).await
    }
}

fn spawn_udp_flow<S>(stream: S, flow_io: VmessEstablishedUdpFlow) -> VmessUdpFlowHandle
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
{
    let (send_tx, send_rx) = mpsc::channel::<VmessUdpFlowSend>(32);
    let (responses, _) = broadcast::channel::<VmessUdpFlowResponse>(32);
    spawn_udp_flow_task(stream, send_rx, responses.clone(), flow_io);
    VmessUdpFlowHandle {
        sender: VmessUdpFlowSender { send_tx },
        responses,
    }
}

pub fn start_udp_flow<S>(stream: S) -> VmessUdpFlowConnection
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
{
    VmessUdpFlowConnection::new(spawn_udp_flow(stream, VmessEstablishedUdpFlow::default()))
}

fn spawn_udp_flow_task<S>(
    mut stream: S,
    mut send_rx: mpsc::Receiver<VmessUdpFlowSend>,
    responses: VmessUdpFlowResponses,
    flow_io: VmessEstablishedUdpFlow,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
{
    tokio::spawn(async move {
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            tokio::select! {
                to_send = send_rx.recv() => {
                    match to_send {
                        Some(request) => {
                            let (target, port, payload) = request.packet.into_parts();
                            let result = flow_io
                                .write_packet_tokio(&mut stream, &target, port, &payload)
                                .await;
                            let should_break = result.is_err();
                            let _ = request.result_tx.send(result);
                            if should_break {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                read = flow_io.read_packet_tokio(&mut stream, &mut buffer) => {
                    match read {
                        Ok(Some(packet)) => {
                            let _ = responses.send(packet.into_parts());
                        }
                        Ok(None) => break,
                        Err(_) => break,
                    }
                }
            }
        }
    });
}

async fn establish_udp_flow<S>(
    stream: S,
    session: &Session,
    identity: VmessUdpIdentity,
) -> Result<(VmessAeadStream<S>, VmessEstablishedUdpFlow), Error>
where
    S: AsyncSocket,
{
    let stream = establish_udp_flow_stream(stream, session, identity).await?;
    Ok((stream, VmessEstablishedUdpFlow { io: VmessUdpFlowIo }))
}

async fn establish_udp_flow_with_resume<S>(
    stream: S,
    session: &Session,
    resume: &VmessUdpFlowResume,
) -> Result<VmessUdpFlowConnection, Error>
where
    S: AsyncSocket + tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static,
{
    let (stream, flow_io) = establish_udp_flow(stream, session, resume.identity()).await?;
    Ok(VmessUdpFlowConnection::new(spawn_udp_flow(stream, flow_io)))
}

pub(crate) fn build_udp_packet(
    address: &Address,
    port: u16,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    let mut body = Vec::with_capacity(8 + payload.len());
    write_address(&mut body, address)?;
    body.extend_from_slice(&port.to_be_bytes());
    body.extend_from_slice(payload);

    if body.len() > u16::MAX as usize {
        return Err(Error::Protocol("vmess udp packet too large"));
    }

    let mut packet = Vec::with_capacity(2 + body.len());
    packet.extend_from_slice(&(body.len() as u16).to_be_bytes());
    packet.extend_from_slice(&body);
    Ok(packet)
}

pub(crate) fn encode_udp_response(
    mode: VmessUdpPayloadMode,
    target: &Address,
    port: u16,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    match mode {
        VmessUdpPayloadMode::VmessPacket => build_udp_packet(target, port, payload),
        VmessUdpPayloadMode::RawDatagram => Ok(payload.to_vec()),
    }
}

pub(crate) fn encode_udp_flow_packet(
    target: &Address,
    port: u16,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    build_udp_packet(target, port, payload)
}

pub(crate) fn decode_udp_flow_packet(packet: &[u8]) -> Result<VmessUdpPacket, Error> {
    parse_udp_packet(packet)
}

pub(crate) fn encode_mux_udp_response(
    mux_session_id: u16,
    mode: VmessUdpPayloadMode,
    target: &Address,
    port: u16,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    let payload = encode_udp_response(mode, target, port, payload)?;
    crate::mux::encode_keep_stream(mux_session_id, &payload)
}

fn encode_inbound_udp_response(
    mode: VmessUdpPayloadMode,
    target: &Address,
    port: u16,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    encode_udp_response(mode, target, port, payload)
}

fn encode_inbound_mux_udp_response(
    mux_session_id: u16,
    mode: VmessUdpPayloadMode,
    target: &Address,
    port: u16,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    encode_mux_udp_response(mux_session_id, mode, target, port, payload)
}

fn decode_inbound_udp_payload(
    state: VmessUdpPayloadState,
    default_target: &Address,
    default_port: u16,
    payload: &[u8],
) -> Result<VmessInboundUdpPayload, Error> {
    match state {
        VmessUdpPayloadState::Unknown => match parse_udp_packet(payload) {
            Ok(packet) => {
                let (target, port, payload) = packet.into_parts();
                Ok(VmessInboundUdpPayload::new(
                    VmessUdpPayloadState::Mode(VmessUdpPayloadMode::VmessPacket),
                    target,
                    port,
                    payload,
                ))
            }
            Err(_) => Ok(VmessInboundUdpPayload::new(
                VmessUdpPayloadState::Mode(VmessUdpPayloadMode::RawDatagram),
                default_target.clone(),
                default_port,
                payload.to_vec(),
            )),
        },
        VmessUdpPayloadState::Mode(VmessUdpPayloadMode::VmessPacket) => {
            let packet = parse_udp_packet(payload)?;
            let (target, port, payload) = packet.into_parts();
            Ok(VmessInboundUdpPayload::new(state, target, port, payload))
        }
        VmessUdpPayloadState::Mode(VmessUdpPayloadMode::RawDatagram) => {
            Ok(VmessInboundUdpPayload::new(
                state,
                default_target.clone(),
                default_port,
                payload.to_vec(),
            ))
        }
    }
}

fn decode_inbound_udp_datagram(
    state: VmessUdpPayloadState,
    default_target: &Address,
    default_port: u16,
    payload: &[u8],
) -> Result<VmessInboundUdpPayload, Error> {
    decode_inbound_udp_payload(state, default_target, default_port, payload)
}

#[derive(Debug, Default, Clone, Copy)]
struct VmessInboundUdpCodec;

impl VmessInboundUdpCodec {
    pub fn response_mode(&self, state: VmessUdpPayloadState) -> VmessUdpPayloadMode {
        match state {
            VmessUdpPayloadState::Unknown
            | VmessUdpPayloadState::Mode(VmessUdpPayloadMode::VmessPacket) => {
                VmessUdpPayloadMode::VmessPacket
            }
            VmessUdpPayloadState::Mode(VmessUdpPayloadMode::RawDatagram) => {
                VmessUdpPayloadMode::RawDatagram
            }
        }
    }

    pub fn encode_response(
        &self,
        mode: VmessUdpPayloadMode,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Vec<u8>, Error> {
        encode_inbound_udp_response(mode, target, port, payload)
    }

    pub fn encode_response_for_state(
        &self,
        state: VmessUdpPayloadState,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Vec<u8>, Error> {
        self.encode_response(self.response_mode(state), target, port, payload)
    }

    pub async fn write_response_tokio<W>(
        &self,
        writer: &mut W,
        state: VmessUdpPayloadState,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        let packet = self.encode_response_for_state(state, target, port, payload)?;
        let len = packet.len();
        tokio::io::AsyncWriteExt::write_all(writer, &packet)
            .await
            .map_err(|_| Error::Io("failed to write VMess UDP response"))?;
        tokio::io::AsyncWriteExt::flush(writer)
            .await
            .map_err(|_| Error::Io("failed to flush VMess UDP response"))?;
        Ok(len)
    }

    pub fn encode_mux_response(
        &self,
        mux_session_id: u16,
        mode: VmessUdpPayloadMode,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Vec<u8>, Error> {
        encode_inbound_mux_udp_response(mux_session_id, mode, target, port, payload)
    }

    pub fn encode_mux_response_for_state(
        &self,
        mux_session_id: u16,
        state: VmessUdpPayloadState,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Vec<u8>, Error> {
        self.encode_mux_response(
            mux_session_id,
            self.response_mode(state),
            target,
            port,
            payload,
        )
    }

    pub fn decode_datagram(
        &self,
        state: VmessUdpPayloadState,
        default_target: &Address,
        default_port: u16,
        payload: &[u8],
    ) -> Result<VmessInboundUdpPayload, Error> {
        decode_inbound_udp_datagram(state, default_target, default_port, payload)
    }
}

pub(crate) fn parse_udp_packet(packet: &[u8]) -> Result<VmessUdpPacket, Error> {
    if packet.len() < 2 {
        return Err(Error::Protocol("vmess udp packet too short"));
    }

    let body_len = u16::from_be_bytes([packet[0], packet[1]]) as usize;
    if packet.len() < 2 + body_len {
        return Err(Error::Protocol("vmess udp packet truncated"));
    }
    let body = &packet[2..2 + body_len];

    let (target, offset) = parse_address_body(body)?;
    if body.len() < offset + 2 {
        return Err(Error::Protocol("vmess udp packet missing port"));
    }
    let port = u16::from_be_bytes([body[offset], body[offset + 1]]);
    let payload = body[offset + 2..].to_vec();

    Ok(VmessUdpPacket {
        target,
        port,
        payload,
    })
}

fn parse_address_body(body: &[u8]) -> Result<(Address, usize), Error> {
    if body.is_empty() {
        return Err(Error::Protocol("vmess udp empty address body"));
    }

    let atyp = body[0];
    match atyp {
        0x01 => {
            if body.len() < 5 {
                return Err(Error::Protocol("vmess udp truncated ipv4"));
            }
            Ok((parse_address_from_bytes(atyp, &body[1..5])?, 5))
        }
        0x02 => {
            if body.len() < 2 {
                return Err(Error::Protocol("vmess udp truncated domain length"));
            }
            let len = body[1] as usize;
            let end = 2 + len;
            if body.len() < end {
                return Err(Error::Protocol("vmess udp truncated domain"));
            }
            Ok((parse_address_from_bytes(atyp, &body[1..end])?, end))
        }
        0x03 => {
            if body.len() < 17 {
                return Err(Error::Protocol("vmess udp truncated ipv6"));
            }
            Ok((parse_address_from_bytes(atyp, &body[1..17])?, 17))
        }
        _ => Err(Error::Protocol("vmess udp unknown address type")),
    }
}
