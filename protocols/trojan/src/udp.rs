#[cfg(feature = "tokio")]
use core::future::Future;
use std::string::String;

#[cfg(feature = "tokio")]
use std::io;

#[cfg(feature = "tokio")]
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
#[cfg(feature = "tokio")]
use tokio::sync::{broadcast, mpsc};
#[cfg(feature = "tokio")]
use zero_core::UdpFlowPacket;
use zero_core::{Address, Error, InboundUdpDispatch, ProtocolType, Session, StreamUdpResponder};
#[cfg(feature = "tokio")]
use zero_core::{MuxUdpDecodeFailure, MuxUdpResponder};
use zero_traits::{AsyncSocket, UdpPacketStreamFraming, UdpPacketTunnelProtocol};

use crate::outbound::{
    resolved_tls_profile_from_parts, OwnedTrojanResolvedTlsProfile, TrojanOutbound,
    TrojanResolvedTlsProfile,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct TrojanInboundUdpRequest {
    target: Address,
    port: u16,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TrojanInboundUdpDispatchParts {
    target: Address,
    port: u16,
    payload: Vec<u8>,
    client_session_id: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct TrojanInboundUdpClientResponse<'a> {
    target: &'a Address,
    port: u16,
    payload: &'a [u8],
}

impl<'a> TrojanInboundUdpClientResponse<'a> {
    fn new(target: &'a Address, port: u16, payload: &'a [u8]) -> Self {
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

impl TrojanInboundUdpDispatchParts {
    fn into_inbound_dispatch(self) -> InboundUdpDispatch {
        InboundUdpDispatch::new(
            ProtocolType::new("trojan"),
            self.target,
            self.port,
            self.payload,
            self.client_session_id,
        )
    }
}

impl TrojanInboundUdpRequest {
    fn from_packet(packet: TrojanUdpPacket) -> Self {
        let (target, port, payload) = packet.into_parts();
        Self {
            target,
            port,
            payload,
        }
    }

    fn into_parts(self) -> (Address, u16, Vec<u8>) {
        (self.target, self.port, self.payload)
    }

    fn into_dispatch_parts(self) -> TrojanInboundUdpDispatchParts {
        let (target, port, payload) = self.into_parts();
        TrojanInboundUdpDispatchParts {
            target,
            port,
            payload,
            client_session_id: None,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct TrojanInboundUdpSession {
    codec: TrojanInboundUdpCodec,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TrojanInboundUdpResponder {
    session: TrojanInboundUdpSession,
}

#[cfg(feature = "tokio")]
pub(crate) struct TrojanInboundMuxUdpResponder {
    target: Address,
    port: u16,
    writer: crate::mux::TrojanInboundMuxWriter,
    mux_session_id: u16,
}

#[cfg(feature = "tokio")]
impl TrojanInboundMuxUdpResponder {
    pub(crate) fn new(
        target: Address,
        port: u16,
        writer: crate::mux::TrojanInboundMuxWriter,
        mux_session_id: u16,
    ) -> Self {
        Self {
            target,
            port,
            writer,
            mux_session_id,
        }
    }

    fn decode(&self, payload: &[u8]) -> InboundUdpDispatch {
        InboundUdpDispatch::new(
            ProtocolType::new("trojan"),
            self.target.clone(),
            self.port,
            payload.to_vec(),
            None,
        )
    }

    fn write_response(&self, target: &Address, port: u16, payload: &[u8]) -> Result<usize, Error> {
        if target != &self.target || port != self.port {
            return Err(Error::Protocol(
                "trojan mux UDP response target differs from the requested target",
            ));
        }
        self.writer.data(self.mux_session_id, payload)
    }
}

#[cfg(feature = "tokio")]
impl MuxUdpResponder for TrojanInboundMuxUdpResponder {
    fn decode_inbound_dispatch(&mut self, payload: &[u8]) -> Result<InboundUdpDispatch, Error> {
        Ok(self.decode(payload))
    }

    fn write_response_for_target(
        &mut self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error> {
        self.write_response(target, port, payload)
    }

    fn end_inbound_stream(&mut self) -> Result<usize, Error> {
        self.writer.end_inbound_stream(self.mux_session_id)
    }

    fn decode_failure(&self) -> MuxUdpDecodeFailure {
        MuxUdpDecodeFailure::End
    }
}

impl TrojanInboundUdpSession {
    fn new() -> Self {
        Self::default()
    }

    async fn read_request<S>(&self, stream: &mut S) -> Result<TrojanInboundUdpRequest, Error>
    where
        S: AsyncSocket,
    {
        self.codec
            .read_packet(stream)
            .await
            .map(TrojanInboundUdpRequest::from_packet)
    }

    async fn read_dispatch_parts<S>(
        &self,
        stream: &mut S,
    ) -> Result<TrojanInboundUdpDispatchParts, Error>
    where
        S: AsyncSocket,
    {
        self.read_request(stream)
            .await
            .map(TrojanInboundUdpRequest::into_dispatch_parts)
    }

    async fn read_inbound_dispatch<S>(&self, stream: &mut S) -> Result<InboundUdpDispatch, Error>
    where
        S: AsyncSocket,
    {
        self.read_dispatch_parts(stream)
            .await
            .map(TrojanInboundUdpDispatchParts::into_inbound_dispatch)
    }

    async fn write_response<S>(
        &self,
        stream: &mut S,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error>
    where
        S: AsyncSocket,
    {
        self.codec
            .write_response(stream, target, port, payload)
            .await
    }

    async fn write_client_response<S>(
        &self,
        stream: &mut S,
        response: TrojanInboundUdpClientResponse<'_>,
    ) -> Result<usize, Error>
    where
        S: AsyncSocket,
    {
        self.write_response(
            stream,
            response.target(),
            response.port(),
            response.payload(),
        )
        .await
    }

    async fn write_client_response_for_target<S>(
        &self,
        stream: &mut S,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error>
    where
        S: AsyncSocket,
    {
        self.write_client_response(
            stream,
            TrojanInboundUdpClientResponse::new(target, port, payload),
        )
        .await
    }
}

impl TrojanInboundUdpResponder {
    fn new(session: TrojanInboundUdpSession) -> Self {
        Self { session }
    }

    pub(crate) async fn read_inbound_dispatch<S>(
        &self,
        stream: &mut S,
    ) -> Result<InboundUdpDispatch, Error>
    where
        S: AsyncSocket,
    {
        self.session.read_inbound_dispatch(stream).await
    }

    pub(crate) async fn write_response_for_target<S>(
        &self,
        stream: &mut S,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error>
    where
        S: AsyncSocket,
    {
        self.session
            .write_client_response_for_target(stream, target, port, payload)
            .await
    }
}

#[async_trait::async_trait]
impl<S> StreamUdpResponder<S> for TrojanInboundUdpResponder
where
    S: AsyncSocket,
{
    async fn read_inbound_dispatch(
        &mut self,
        client: &mut S,
    ) -> Result<Option<InboundUdpDispatch>, Error> {
        TrojanInboundUdpResponder::read_inbound_dispatch(self, client)
            .await
            .map(Some)
    }

    async fn write_response_for_target(
        &mut self,
        client: &mut S,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error> {
        TrojanInboundUdpResponder::write_response_for_target(self, client, target, port, payload)
            .await
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct TrojanInboundUdpCodec;

impl TrojanInboundUdpCodec {
    async fn read_packet<S>(&self, stream: &mut S) -> Result<TrojanUdpPacket, Error>
    where
        S: AsyncSocket,
    {
        let (target, port, payload) = crate::shared::read_udp_packet(stream).await?;
        Ok(TrojanUdpPacket::new(target, port, payload))
    }

    async fn write_response<S>(
        &self,
        stream: &mut S,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error>
    where
        S: AsyncSocket,
    {
        crate::shared::write_udp_packet(stream, target, port, payload).await
    }
}

impl crate::inbound::TrojanInbound {
    fn udp_session(&self) -> TrojanInboundUdpSession {
        TrojanInboundUdpSession::new()
    }

    fn udp_responder(&self) -> TrojanInboundUdpResponder {
        TrojanInboundUdpResponder::new(self.udp_session())
    }

    pub(crate) fn accept_udp_session(&self) -> TrojanInboundUdpResponder {
        self.udp_responder()
    }
}

/// Target parameters for Trojan UDP packet tunnel over a connected stream.
#[derive(Debug, Clone, Copy)]
struct TrojanUdpPacketTunnelTarget<'a> {
    pub session: &'a Session,
    pub password: &'a str,
}

impl<'a> UdpPacketTunnelProtocol<TrojanUdpPacketTunnelTarget<'a>> for TrojanOutbound {
    type Error = Error;

    async fn establish_udp_packet_tunnel<S>(
        &self,
        stream: &mut S,
        target: &TrojanUdpPacketTunnelTarget<'a>,
    ) -> Result<(), Self::Error>
    where
        S: AsyncSocket,
    {
        let request =
            build_udp_request(target.password, &target.session.target, target.session.port)?;
        stream
            .write_all(&request)
            .await
            .map_err(|_| Error::Io("trojan: write udp request failed"))
    }
}

fn build_udp_request(password: &str, addr: &Address, port: u16) -> Result<Vec<u8>, Error> {
    crate::shared::build_request(password, addr, port, crate::shared::CMD_UDP)
}

/// One Trojan UDP packet carried over a connected stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrojanUdpPacket {
    target: Address,
    port: u16,
    payload: Vec<u8>,
}

impl TrojanUdpPacket {
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

#[cfg(feature = "tokio")]
#[derive(Debug, Default, Clone, Copy)]
struct TrojanUdpFlowIo;

#[cfg(feature = "tokio")]
type TrojanUdpFlowResponses = broadcast::Sender<UdpFlowPacket>;

#[cfg(feature = "tokio")]
pub type TrojanUdpFlowResponseReceiver = broadcast::Receiver<UdpFlowPacket>;

#[cfg(feature = "tokio")]
#[derive(Clone)]
struct TrojanUdpFlowSender {
    send_tx: mpsc::Sender<UdpFlowPacket>,
    fixed_target: Option<(Address, u16)>,
    max_payload_len: Option<usize>,
}

#[cfg(feature = "tokio")]
struct TrojanUdpFlowHandle {
    sender: TrojanUdpFlowSender,
    responses: TrojanUdpFlowResponses,
}

#[cfg(feature = "tokio")]
#[derive(Clone)]
struct TrojanUdpFlowSession {
    sender: TrojanUdpFlowSender,
    responses: TrojanUdpFlowResponses,
}

#[cfg(feature = "tokio")]
impl TrojanUdpFlowSession {
    fn new(handle: TrojanUdpFlowHandle) -> Self {
        Self {
            sender: handle.sender,
            responses: handle.responses,
        }
    }

    async fn send(&self, target: &Address, port: u16, payload: &[u8]) -> Result<usize, Error> {
        self.sender.send(target, port, payload).await
    }

    fn subscribe_responses(&self) -> TrojanUdpFlowResponseReceiver {
        self.responses.subscribe()
    }
}

#[cfg(feature = "tokio")]
#[derive(Clone)]
pub struct TrojanUdpFlowConnection {
    session: TrojanUdpFlowSession,
}

#[cfg(feature = "tokio")]
impl TrojanUdpFlowConnection {
    fn new(session: TrojanUdpFlowSession) -> Self {
        Self { session }
    }

    pub async fn send(&self, target: &Address, port: u16, payload: &[u8]) -> Result<usize, Error> {
        self.session.send(target, port, payload).await
    }

    pub fn subscribe_responses(&self) -> TrojanUdpFlowResponseReceiver {
        self.session.subscribe_responses()
    }
}

#[cfg(feature = "tokio")]
impl TrojanUdpFlowSender {
    async fn send(&self, target: &Address, port: u16, payload: &[u8]) -> Result<usize, Error> {
        if self
            .fixed_target
            .as_ref()
            .is_some_and(|(expected_target, expected_port)| {
                target != expected_target || port != *expected_port
            })
        {
            return Err(Error::Protocol(
                "Trojan MUX UDP substream target cannot change",
            ));
        }
        if self
            .max_payload_len
            .is_some_and(|max_payload_len| payload.len() > max_payload_len)
        {
            return Err(Error::Protocol(
                "Trojan MUX UDP payload exceeds frame limit",
            ));
        }
        let packet = UdpFlowPacket::from_parts(target, port, payload);
        let packet_len = packet.payload.len();
        self.send_tx
            .send(packet)
            .await
            .map_err(|_| Error::Io("trojan udp flow closed"))?;
        Ok(packet_len)
    }
}

#[cfg(feature = "tokio")]
impl TrojanUdpFlowIo {
    async fn establish<S>(
        &self,
        stream: &mut S,
        session: &Session,
        password: &str,
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        establish_udp_packet_tunnel(stream, session, password).await
    }

    async fn establish_with_resume<S>(
        &self,
        stream: &mut S,
        session: &Session,
        resume: &TrojanUdpFlowResume,
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        resume.establish_udp_tunnel(self, stream, session).await
    }

    async fn write_packet<S>(
        &self,
        stream: &mut S,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        write_udp_flow_packet(stream, target, port, payload).await
    }

    async fn read_packet<S>(&self, stream: &mut S) -> Result<TrojanUdpPacket, Error>
    where
        S: AsyncSocket,
    {
        read_udp_flow_packet(stream).await
    }

    async fn read_flow_packet<S>(&self, stream: &mut S) -> Result<UdpFlowPacket, Error>
    where
        S: AsyncSocket,
    {
        let packet = self.read_packet(stream).await?;
        let (target, port, payload) = packet.into_parts();
        Ok(UdpFlowPacket::new(target, port, payload))
    }

    async fn write_flow_packet<S>(
        &self,
        stream: &mut S,
        packet: &UdpFlowPacket,
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.write_packet(stream, &packet.target, packet.port, &packet.payload)
            .await
    }
}

#[cfg(feature = "tokio")]
fn spawn_udp_flow<S>(stream: S) -> TrojanUdpFlowHandle
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
{
    let (read_half, write_half) = tokio::io::split(stream);
    let (send_tx, send_rx) = mpsc::channel::<UdpFlowPacket>(32);
    let (recv_tx, _) = broadcast::channel::<UdpFlowPacket>(32);

    spawn_send_task(send_rx, WriteOnlySocket(write_half));
    spawn_recv_task(ReadOnlySocket(read_half), recv_tx.clone());

    TrojanUdpFlowHandle {
        sender: TrojanUdpFlowSender {
            send_tx,
            fixed_target: None,
            max_payload_len: None,
        },
        responses: recv_tx,
    }
}

#[cfg(feature = "tokio")]
pub(crate) fn start_mux_udp_flow<S>(
    stream: S,
    target: Address,
    port: u16,
) -> TrojanUdpFlowConnection
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
{
    let (mut read_half, mut write_half) = tokio::io::split(stream);
    let (send_tx, mut send_rx) = mpsc::channel::<UdpFlowPacket>(32);
    let (recv_tx, _) = broadcast::channel::<UdpFlowPacket>(32);
    let recv_task_tx = recv_tx.clone();
    let send_target = target.clone();
    let fixed_target = target.clone();

    tokio::spawn(async move {
        while let Some(packet) = send_rx.recv().await {
            if packet.target != send_target
                || packet.port != port
                || packet.payload.len() > crate::mux::MUX_MAX_DATA_LEN
            {
                break;
            }
            if write_half.write_all(&packet.payload).await.is_err()
                || write_half.flush().await.is_err()
            {
                break;
            }
        }
        let _ = write_half.shutdown().await;
    });

    tokio::spawn(async move {
        let mut payload = vec![0_u8; crate::mux::MUX_MAX_DATA_LEN];
        loop {
            match read_half.read(&mut payload).await {
                Ok(0) | Err(_) => break,
                Ok(read) => {
                    if recv_task_tx
                        .send(UdpFlowPacket::new(
                            target.clone(),
                            port,
                            payload[..read].to_vec(),
                        ))
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
    });

    TrojanUdpFlowConnection::new(TrojanUdpFlowSession::new(TrojanUdpFlowHandle {
        sender: TrojanUdpFlowSender {
            send_tx,
            fixed_target: Some((fixed_target, port)),
            max_payload_len: Some(crate::mux::MUX_MAX_DATA_LEN),
        },
        responses: recv_tx,
    }))
}

#[cfg(feature = "tokio")]
async fn establish_udp_flow_with_resume<S>(
    mut stream: S,
    session: &Session,
    resume: &TrojanUdpFlowResume,
) -> Result<TrojanUdpFlowConnection, Error>
where
    S: AsyncSocket + tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static,
{
    let flow_io = TrojanUdpFlowIo;
    flow_io
        .establish_with_resume(&mut stream, session, resume)
        .await?;
    Ok(TrojanUdpFlowConnection::new(TrojanUdpFlowSession::new(
        spawn_udp_flow(stream),
    )))
}

#[cfg(feature = "tokio")]
fn spawn_send_task<S>(
    mut send_rx: mpsc::Receiver<UdpFlowPacket>,
    mut send_stream: WriteOnlySocket<S>,
) where
    S: tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
{
    tokio::spawn(async move {
        let flow_io = TrojanUdpFlowIo;
        while let Some(packet) = send_rx.recv().await {
            if flow_io
                .write_flow_packet(&mut send_stream, &packet)
                .await
                .is_err()
            {
                break;
            }
        }
    });
}

#[cfg(feature = "tokio")]
fn spawn_recv_task<S>(mut recv_stream: ReadOnlySocket<S>, recv_tx: broadcast::Sender<UdpFlowPacket>)
where
    S: tokio::io::AsyncRead + Send + Sync + Unpin + 'static,
{
    tokio::spawn(async move {
        let flow_io = TrojanUdpFlowIo;
        while let Ok(packet) = flow_io.read_flow_packet(&mut recv_stream).await {
            if recv_tx.send(packet).is_err() {
                break;
            }
        }
    });
}

#[cfg(feature = "tokio")]
struct ReadOnlySocket<S>(ReadHalf<S>);

#[cfg(feature = "tokio")]
impl<S> AsyncSocket for ReadOnlySocket<S>
where
    S: tokio::io::AsyncRead + Send + Sync + Unpin,
{
    type Error = io::Error;

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.0.read(buf).await
    }

    async fn write_all(&mut self, _buf: &[u8]) -> Result<(), Self::Error> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "read-only socket cannot write",
        ))
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[cfg(feature = "tokio")]
struct WriteOnlySocket<S>(WriteHalf<S>);

#[cfg(feature = "tokio")]
impl<S> AsyncSocket for WriteOnlySocket<S>
where
    S: tokio::io::AsyncWrite + Send + Sync + Unpin,
{
    type Error = io::Error;

    async fn read(&mut self, _buf: &mut [u8]) -> Result<usize, Self::Error> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "write-only socket cannot read",
        ))
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        self.0.write_all(buf).await?;
        self.0.flush().await
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        self.0.shutdown().await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TrojanUdpFlowResume {
    password: String,
    sni: Option<String>,
    insecure: bool,
    client_fingerprint: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrojanUdpFlowMode {
    Direct,
    Relay,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrojanUdpFlowPlan {
    resume: TrojanUdpFlowResume,
    mode: TrojanUdpFlowMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedTrojanUdpFlowPlan {
    plan: TrojanUdpFlowPlan,
    mux: Option<TrojanUdpMuxConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TrojanUdpMuxConfig {
    server: String,
    port: u16,
    password: String,
    sni: Option<String>,
    insecure: bool,
    client_fingerprint: Option<String>,
    concurrency: u32,
    idle_timeout_secs: Option<u64>,
    response_backlog: crate::validation::MuxResponseBacklogPolicy,
}

impl TrojanUdpFlowPlan {
    fn new(resume: TrojanUdpFlowResume, mode: TrojanUdpFlowMode) -> Self {
        Self { resume, mode }
    }

    pub(crate) fn direct_from_config(
        password: &str,
        sni: Option<&str>,
        insecure: bool,
        client_fingerprint: Option<&str>,
    ) -> Self {
        TrojanUdpFlowPlan::new(
            udp_flow_resume_from_config(password, sni, insecure, client_fingerprint),
            TrojanUdpFlowMode::Direct,
        )
    }

    pub(crate) fn relay_from_config(
        password: &str,
        sni: Option<&str>,
        insecure: bool,
        client_fingerprint: Option<&str>,
    ) -> Self {
        TrojanUdpFlowPlan::new(
            udp_flow_resume_from_config(password, sni, insecure, client_fingerprint),
            TrojanUdpFlowMode::Relay,
        )
    }

    pub fn connector_flow(
        &self,
        server: &str,
        port: u16,
        session_id: u64,
    ) -> TrojanUdpConnectorFlow {
        TrojanUdpConnectorFlow {
            cache_key: self.flow_cache_key(server, port, session_id),
            requires_relay_upstream: self.flow_requires_relay_upstream(),
        }
    }

    pub(crate) fn tls_profile<'a>(
        &'a self,
        fallback_server_name: Option<&'a str>,
    ) -> TrojanResolvedTlsProfile<'a> {
        self.resume().tls_profile(fallback_server_name)
    }

    pub fn owned_tls_profile(
        &self,
        fallback_server_name: Option<&str>,
    ) -> OwnedTrojanResolvedTlsProfile {
        self.tls_profile(fallback_server_name).into_owned()
    }

    fn resume(&self) -> &TrojanUdpFlowResume {
        &self.resume
    }

    fn mode(&self) -> TrojanUdpFlowMode {
        self.mode
    }

    fn flow_requires_relay_upstream(&self) -> bool {
        matches!(self.mode(), TrojanUdpFlowMode::Relay)
    }

    fn flow_cache_key(&self, server: &str, port: u16, session_id: u64) -> String {
        if self.flow_requires_relay_upstream() {
            return format!("relay|session:{session_id}");
        }
        format!("leaf|{server}:{port}|password:{}", self.resume.password)
    }

    #[cfg(feature = "tokio")]
    pub(crate) async fn open_udp_flow_with_transport<S, OpenStream, OpenStreamFut, E>(
        &self,
        session: &Session,
        open_stream: OpenStream,
    ) -> Result<TrojanUdpFlowConnection, E>
    where
        S: AsyncSocket + tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static,
        OpenStream: FnOnce() -> OpenStreamFut,
        OpenStreamFut: Future<Output = Result<S, E>>,
        E: From<Error>,
    {
        let stream = open_stream().await?;
        establish_udp_flow_with_resume(stream, session, self.resume())
            .await
            .map_err(E::from)
    }
}

impl PreparedTrojanUdpFlowPlan {
    pub(crate) fn new(plan: TrojanUdpFlowPlan) -> Self {
        Self { plan, mux: None }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn with_mux_config(
        plan: TrojanUdpFlowPlan,
        server: &str,
        port: u16,
        password: &str,
        sni: Option<&str>,
        insecure: bool,
        client_fingerprint: Option<&str>,
        concurrency: Option<u32>,
        idle_timeout_secs: Option<u64>,
        response_backlog: crate::validation::MuxResponseBacklogPolicy,
    ) -> Self {
        let mux = concurrency.map(|concurrency| TrojanUdpMuxConfig {
            server: server.to_owned(),
            port,
            password: password.to_owned(),
            sni: sni.map(ToOwned::to_owned),
            insecure,
            client_fingerprint: client_fingerprint.map(ToOwned::to_owned),
            concurrency,
            idle_timeout_secs,
            response_backlog,
        });
        Self { plan, mux }
    }

    pub fn connector_flow(
        &self,
        server: &str,
        port: u16,
        session_id: u64,
    ) -> TrojanUdpConnectorFlow {
        self.plan.connector_flow(server, port, session_id)
    }

    pub fn owned_tls_profile(
        &self,
        fallback_server_name: Option<&str>,
    ) -> OwnedTrojanResolvedTlsProfile {
        self.plan.owned_tls_profile(fallback_server_name)
    }

    #[cfg(feature = "tokio")]
    pub async fn open_udp_flow_with_transport<S, OpenStream, OpenStreamFut, E>(
        &self,
        session: &Session,
        fallback_server_name: Option<&str>,
        open_stream: OpenStream,
    ) -> Result<TrojanUdpFlowConnection, E>
    where
        S: AsyncSocket + tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static,
        OpenStream: FnOnce(OwnedTrojanResolvedTlsProfile) -> OpenStreamFut,
        OpenStreamFut: Future<Output = Result<S, E>>,
        E: From<Error>,
    {
        let tls_profile = self.plan.owned_tls_profile(fallback_server_name);
        let stream = open_stream(tls_profile).await?;
        self.plan
            .open_udp_flow_with_transport(session, move || async move { Ok(stream) })
            .await
    }

    #[cfg(feature = "tokio")]
    pub async fn open_udp_flow_with_transport_or_mux<S, OpenStream, OpenStreamFut, E>(
        &self,
        session: &Session,
        fallback_server_name: Option<&str>,
        mux_pool: &crate::mux::TrojanMuxConnectionPool,
        open_stream: OpenStream,
    ) -> Result<TrojanUdpFlowConnection, E>
    where
        S: AsyncSocket
            + tokio::io::AsyncRead
            + tokio::io::AsyncWrite
            + Send
            + Sync
            + Unpin
            + 'static,
        OpenStream: FnOnce(OwnedTrojanResolvedTlsProfile) -> OpenStreamFut,
        OpenStreamFut: Future<Output = Result<S, E>>,
        E: From<Error>,
    {
        let tls_profile = self.plan.owned_tls_profile(fallback_server_name);
        if let Some(mux) = &self.mux {
            let key = crate::mux::pool_key_from_config(
                &mux.server,
                mux.port,
                &mux.password,
                mux.sni.as_deref(),
                mux.insecure,
                mux.client_fingerprint.as_deref(),
                mux.idle_timeout_secs,
                mux.response_backlog,
            );
            let stream = mux_pool
                .open_udp_stream(
                    key,
                    mux.concurrency,
                    session.target.clone(),
                    session.port,
                    move || open_stream(tls_profile),
                )
                .await?;
            return Ok(start_mux_udp_flow(
                stream,
                session.target.clone(),
                session.port,
            ));
        }

        let stream = open_stream(tls_profile).await?;
        self.plan
            .open_udp_flow_with_transport(session, move || async move { Ok(stream) })
            .await
    }
}

impl TrojanUdpFlowResume {
    fn new(
        password: &str,
        sni: Option<&str>,
        insecure: bool,
        client_fingerprint: Option<&str>,
    ) -> Self {
        Self {
            password: password.to_owned(),
            sni: sni.map(ToOwned::to_owned),
            insecure,
            client_fingerprint: client_fingerprint.map(ToOwned::to_owned),
        }
    }

    fn tls_profile<'a>(
        &'a self,
        fallback_server_name: Option<&'a str>,
    ) -> TrojanResolvedTlsProfile<'a> {
        resolved_tls_profile_from_parts(
            self.sni.as_deref(),
            self.insecure,
            self.client_fingerprint.as_deref(),
            fallback_server_name,
        )
    }

    #[cfg(feature = "tokio")]
    async fn establish_udp_tunnel<S>(
        &self,
        flow_io: &TrojanUdpFlowIo,
        stream: &mut S,
        session: &Session,
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        flow_io.establish(stream, session, &self.password).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrojanUdpConnectorFlow {
    cache_key: String,
    requires_relay_upstream: bool,
}

impl TrojanUdpConnectorFlow {
    pub fn into_parts(self) -> (String, bool) {
        (self.cache_key, self.requires_relay_upstream)
    }
}

#[derive(Debug, Clone, Copy)]
struct TrojanUdpFlowConfig<'a> {
    password: &'a str,
    sni: Option<&'a str>,
    insecure: bool,
    client_fingerprint: Option<&'a str>,
}

impl<'a> TrojanUdpFlowConfig<'a> {
    fn new(
        password: &'a str,
        sni: Option<&'a str>,
        insecure: bool,
        client_fingerprint: Option<&'a str>,
    ) -> Self {
        Self {
            password,
            sni,
            insecure,
            client_fingerprint,
        }
    }

    fn flow_resume(&self) -> TrojanUdpFlowResume {
        TrojanUdpFlowResume::new(
            self.password,
            self.sni,
            self.insecure,
            self.client_fingerprint,
        )
    }
}

fn udp_flow_resume_from_config(
    password: &str,
    sni: Option<&str>,
    insecure: bool,
    client_fingerprint: Option<&str>,
) -> TrojanUdpFlowResume {
    TrojanUdpFlowConfig::new(password, sni, insecure, client_fingerprint).flow_resume()
}

impl UdpPacketStreamFraming<TrojanUdpPacket> for TrojanOutbound {
    type Error = Error;
    type Decoded = TrojanUdpPacket;

    async fn write_udp_packet<S>(
        &self,
        stream: &mut S,
        packet: &TrojanUdpPacket,
    ) -> Result<(), Self::Error>
    where
        S: AsyncSocket,
    {
        crate::shared::write_udp_packet(stream, packet.target(), packet.port(), packet.payload())
            .await
            .map(|_| ())
    }

    async fn read_udp_packet<S>(&self, stream: &mut S) -> Result<Self::Decoded, Self::Error>
    where
        S: AsyncSocket,
    {
        let (target, port, payload) = crate::shared::read_udp_packet(stream).await?;
        Ok(TrojanUdpPacket::new(target, port, payload))
    }
}

#[cfg(feature = "tokio")]
async fn read_inbound_udp_packet<S>(stream: &mut S) -> Result<TrojanUdpPacket, Error>
where
    S: AsyncSocket,
{
    <TrojanOutbound as UdpPacketStreamFraming<TrojanUdpPacket>>::read_udp_packet(
        &TrojanOutbound,
        stream,
    )
    .await
}

#[cfg(feature = "tokio")]
async fn read_udp_flow_packet<S>(stream: &mut S) -> Result<TrojanUdpPacket, Error>
where
    S: AsyncSocket,
{
    read_inbound_udp_packet(stream).await
}

pub async fn establish_udp_packet_tunnel<S>(
    stream: &mut S,
    session: &Session,
    password: &str,
) -> Result<(), Error>
where
    S: AsyncSocket,
{
    <TrojanOutbound as UdpPacketTunnelProtocol<TrojanUdpPacketTunnelTarget<'_>>>::establish_udp_packet_tunnel(
        &TrojanOutbound,
        stream,
        &TrojanUdpPacketTunnelTarget { session, password },
    )
    .await
}

#[cfg(feature = "tokio")]
async fn write_udp_response<S>(
    stream: &mut S,
    target: &Address,
    port: u16,
    payload: &[u8],
) -> Result<(), Error>
where
    S: AsyncSocket,
{
    let packet = TrojanUdpPacket::new(target.clone(), port, payload.to_vec());
    <TrojanOutbound as UdpPacketStreamFraming<TrojanUdpPacket>>::write_udp_packet(
        &TrojanOutbound,
        stream,
        &packet,
    )
    .await
}

#[cfg(feature = "tokio")]
async fn write_udp_flow_packet<S>(
    stream: &mut S,
    target: &Address,
    port: u16,
    payload: &[u8],
) -> Result<(), Error>
where
    S: AsyncSocket,
{
    write_udp_response(stream, target, port, payload).await
}
