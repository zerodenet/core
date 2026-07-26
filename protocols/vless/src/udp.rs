use alloc::format;
use alloc::string::String;
#[cfg(feature = "reality")]
use alloc::vec;
use alloc::vec::Vec;
#[cfg(feature = "reality")]
use core::future::Future;

#[cfg(feature = "reality")]
use tokio::sync::{broadcast, mpsc, oneshot};
use zero_core::{Address, Error, InboundUdpDispatch, ProtocolType, Session};
#[cfg(feature = "reality")]
use zero_core::{MuxUdpDecodeFailure, MuxUdpResponder, StreamUdpResponder};
use zero_traits::{AsyncSocket, UdpPacketFraming, UdpPacketTunnelProtocol};

use crate::outbound::VlessOutbound;
use crate::shared::{
    parse_uuid, read_response, write_address, ATYP_DOMAIN, ATYP_IPV4, ATYP_IPV6, CMD_UDP,
};

/// Target parameters for VLESS UDP packet tunnel over a connected stream.
#[derive(Debug, Clone, Copy)]
struct VlessUdpPacketTunnelTarget<'a> {
    pub session: &'a Session,
    pub id: &'a [u8; 16],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VlessUdpIdentity {
    pub uuid: [u8; 16],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VlessUdpFlowConfig<'a> {
    identity: VlessUdpIdentity,
    flow: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VlessUdpFlowResume {
    identity: VlessUdpIdentity,
    flow: Option<String>,
    relay_chain: bool,
    xudp_concurrency: Option<u32>,
    mux_idle_timeout_secs: Option<u64>,
    mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VlessUdpFlowMode {
    Direct,
    RelayFinalHop,
    RelayPairedTransport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VlessUdpFlowPlan {
    resume: VlessUdpFlowResume,
    mode: VlessUdpFlowMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedVlessUdpFlowPlan {
    plan: VlessUdpFlowPlan,
    #[cfg(feature = "reality")]
    mux_transport_profile: crate::mux_pool::OwnedMuxTransportProfile,
}

impl VlessUdpFlowPlan {
    fn new(resume: VlessUdpFlowResume, mode: VlessUdpFlowMode) -> Self {
        Self { resume, mode }
    }

    pub(crate) fn direct_from_config(
        id: &str,
        flow: Option<&str>,
        xudp_concurrency: Option<u32>,
        mux_idle_timeout_secs: Option<u64>,
        mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
    ) -> Result<Self, Error> {
        udp_direct_flow_resume_from_config(
            id,
            flow,
            xudp_concurrency,
            mux_idle_timeout_secs,
            mux_response_backlog,
        )
        .map(|resume| VlessUdpFlowPlan::new(resume, VlessUdpFlowMode::Direct))
    }

    pub(crate) fn relay_final_hop_from_config(id: &str) -> Result<Self, Error> {
        udp_relay_flow_resume_from_config(id)
            .map(|resume| VlessUdpFlowPlan::new(resume, VlessUdpFlowMode::RelayFinalHop))
    }

    pub(crate) fn relay_paired_transport_from_config(id: &str) -> Result<Self, Error> {
        udp_relay_flow_resume_from_config(id)
            .map(|resume| VlessUdpFlowPlan::new(resume, VlessUdpFlowMode::RelayPairedTransport))
    }

    pub fn connector_flow(
        &self,
        server: &str,
        port: u16,
        session_id: u64,
    ) -> VlessUdpConnectorFlow {
        self.resume().connector_flow(server, port, session_id)
    }

    fn resume(&self) -> &VlessUdpFlowResume {
        &self.resume
    }

    fn mode(&self) -> VlessUdpFlowMode {
        self.mode
    }

    #[cfg(feature = "reality")]
    pub(crate) async fn open_udp_flow_with_transport_or_mux<S, OpenStream, OpenStreamFut, E>(
        &self,
        session: &Session,
        server: &str,
        port: u16,
        profile: crate::mux_pool::MuxTransportProfile<'_>,
        mux_pool: &crate::mux_pool::MuxConnectionPool,
        open_stream: OpenStream,
    ) -> Result<VlessUdpFlowConnection, E>
    where
        S: AsyncSocket + tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static,
        OpenStream: FnOnce() -> OpenStreamFut,
        OpenStreamFut: Future<Output = Result<S, E>>,
        E: From<Error>,
    {
        if self.mode() != VlessUdpFlowMode::Direct {
            return Err(E::from(Error::Protocol(
                "expected direct VLESS UDP flow plan",
            )));
        }
        let resume = self.resume();
        if let Some(key) = resume.udp_mux_pool_key_from_transport_config(server, port, profile) {
            let global_id = session.id.to_be_bytes();
            let max_concurrency = resume
                .xudp_concurrency()
                .ok_or_else(|| E::from(Error::Config("xudp concurrency is required")))?;
            let (_session_id, up_tx, down_rx) = mux_pool
                .open_udp_stream(key, max_concurrency, global_id, open_stream)
                .await?;
            return Ok(start_mux_udp_flow(up_tx, down_rx));
        }

        let stream = open_stream().await?;
        establish_udp_flow_with_resume(stream, session, resume)
            .await
            .map_err(E::from)
    }

    #[cfg(feature = "reality")]
    pub(crate) async fn open_relay_udp_flow_with_transport<
        S,
        OpenFinalHopTransport,
        OpenFinalHopTransportFut,
        E,
    >(
        &self,
        session: &Session,
        stream: S,
        open_final_hop_transport: OpenFinalHopTransport,
    ) -> Result<VlessUdpFlowConnection, E>
    where
        S: AsyncSocket + tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static,
        OpenFinalHopTransport: FnOnce(S) -> OpenFinalHopTransportFut,
        OpenFinalHopTransportFut: Future<Output = Result<S, E>>,
        E: From<Error>,
    {
        let stream = match self.mode() {
            VlessUdpFlowMode::Direct => {
                return Err(E::from(Error::Protocol(
                    "expected relay VLESS UDP flow plan",
                )));
            }
            VlessUdpFlowMode::RelayFinalHop => open_final_hop_transport(stream).await?,
            VlessUdpFlowMode::RelayPairedTransport => stream,
        };

        establish_udp_flow_with_resume(stream, session, self.resume())
            .await
            .map_err(E::from)
    }
}

impl PreparedVlessUdpFlowPlan {
    #[cfg(not(feature = "reality"))]
    pub(crate) fn new(plan: VlessUdpFlowPlan) -> Self {
        Self { plan }
    }

    #[cfg(feature = "reality")]
    pub(crate) fn with_transport_profile(
        plan: VlessUdpFlowPlan,
        mux_transport_profile: crate::mux_pool::OwnedMuxTransportProfile,
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
    ) -> VlessUdpConnectorFlow {
        self.plan.connector_flow(server, port, session_id)
    }

    #[cfg(feature = "reality")]
    pub async fn open_udp_flow_with_transport_or_mux<S, OpenStream, OpenStreamFut, E>(
        &self,
        session: &Session,
        server: &str,
        port: u16,
        mux_pool: &crate::mux_pool::MuxConnectionPool,
        open_stream: OpenStream,
    ) -> Result<VlessUdpFlowConnection, E>
    where
        S: AsyncSocket + tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static,
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

    #[cfg(feature = "reality")]
    pub async fn open_relay_udp_flow_with_transport<
        S,
        OpenFinalHopTransport,
        OpenFinalHopTransportFut,
        E,
    >(
        &self,
        session: &Session,
        stream: S,
        open_final_hop_transport: OpenFinalHopTransport,
    ) -> Result<VlessUdpFlowConnection, E>
    where
        S: AsyncSocket + tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static,
        OpenFinalHopTransport: FnOnce(S) -> OpenFinalHopTransportFut,
        OpenFinalHopTransportFut: Future<Output = Result<S, E>>,
        E: From<Error>,
    {
        self.plan
            .open_relay_udp_flow_with_transport(session, stream, open_final_hop_transport)
            .await
    }
}

impl VlessUdpFlowResume {
    fn identity(&self) -> VlessUdpIdentity {
        self.identity
    }

    fn mux_flow_enabled(&self) -> bool {
        self.xudp_concurrency.is_some()
    }

    fn xudp_concurrency(&self) -> Option<u32> {
        self.xudp_concurrency
    }

    #[cfg(feature = "reality")]
    fn mux_pool_identity(&self) -> crate::mux_pool::MuxIdentity {
        crate::mux_pool::MuxIdentity::from_uuid(self.identity.uuid)
    }

    #[cfg(feature = "reality")]
    fn udp_mux_pool_key_from_transport_config(
        &self,
        server: &str,
        port: u16,
        profile: crate::mux_pool::MuxTransportProfile<'_>,
    ) -> Option<crate::mux_pool::PoolKey> {
        self.mux_flow_enabled().then(|| {
            crate::mux_pool::pool_key_from_transport_config(
                server,
                port,
                self.mux_pool_identity(),
                profile,
                self.mux_idle_timeout_secs,
                self.mux_response_backlog,
            )
        })
    }

    fn flow_requires_relay_upstream(&self) -> bool {
        self.relay_chain
    }

    fn connector_flow(&self, server: &str, port: u16, session_id: u64) -> VlessUdpConnectorFlow {
        VlessUdpConnectorFlow {
            cache_key: format!(
                "vless:{server}:{port}:{session_id}:relay={}",
                self.relay_chain
            ),
            requires_relay_upstream: self.flow_requires_relay_upstream(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VlessUdpConnectorFlow {
    cache_key: String,
    requires_relay_upstream: bool,
}

impl VlessUdpConnectorFlow {
    pub fn into_parts(self) -> (String, bool) {
        (self.cache_key, self.requires_relay_upstream)
    }
}

impl<'a> VlessUdpFlowConfig<'a> {
    fn new(id: &str, flow: Option<&'a str>) -> Result<Self, Error> {
        Ok(Self {
            identity: parse_udp_identity(id)?,
            flow,
        })
    }

    fn flow_resume(
        &self,
        relay_chain: bool,
        xudp_concurrency: Option<u32>,
        mux_idle_timeout_secs: Option<u64>,
        mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
    ) -> VlessUdpFlowResume {
        VlessUdpFlowResume {
            identity: self.identity,
            flow: self.flow.map(Into::into),
            relay_chain,
            xudp_concurrency,
            mux_idle_timeout_secs,
            mux_response_backlog,
        }
    }
}

fn udp_flow_resume_from_config(
    id: &str,
    flow: Option<&str>,
    relay_chain: bool,
    xudp_concurrency: Option<u32>,
    mux_idle_timeout_secs: Option<u64>,
    mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
) -> Result<VlessUdpFlowResume, Error> {
    VlessUdpFlowConfig::new(id, flow).map(|config| {
        config.flow_resume(
            relay_chain,
            xudp_concurrency,
            mux_idle_timeout_secs,
            mux_response_backlog,
        )
    })
}

fn udp_direct_flow_resume_from_config(
    id: &str,
    flow: Option<&str>,
    xudp_concurrency: Option<u32>,
    mux_idle_timeout_secs: Option<u64>,
    mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
) -> Result<VlessUdpFlowResume, Error> {
    udp_flow_resume_from_config(
        id,
        flow,
        false,
        xudp_concurrency,
        mux_idle_timeout_secs,
        mux_response_backlog,
    )
}

fn udp_relay_flow_resume_from_config(id: &str) -> Result<VlessUdpFlowResume, Error> {
    VlessUdpFlowConfig::new(id, None).map(|config| {
        config.flow_resume(
            true,
            None,
            None,
            crate::mux::MuxResponseBacklogPolicy::default(),
        )
    })
}

fn parse_udp_identity(id: &str) -> Result<VlessUdpIdentity, Error> {
    parse_uuid(id).map(|uuid| VlessUdpIdentity { uuid })
}

async fn establish_udp_flow_stream<S>(
    stream: &mut S,
    session: &Session,
    identity: VlessUdpIdentity,
) -> Result<(), Error>
where
    S: AsyncSocket,
{
    send_udp_request(stream, session, &identity.uuid).await
}

#[cfg(feature = "reality")]
#[derive(Debug, Clone)]
struct VlessEstablishedUdpFlow {
    target: Address,
    port: u16,
}

#[cfg(feature = "reality")]
pub type VlessUdpFlowResponse = (Address, u16, Vec<u8>);

#[cfg(feature = "reality")]
type VlessUdpFlowResponses = broadcast::Sender<VlessUdpFlowResponse>;

#[cfg(feature = "reality")]
pub type VlessUdpFlowResponseReceiver = broadcast::Receiver<VlessUdpFlowResponse>;

#[cfg(feature = "reality")]
struct VlessUdpFlowSend {
    packet: zero_core::UdpFlowPacket,
    result_tx: oneshot::Sender<Result<usize, Error>>,
}

#[cfg(feature = "reality")]
#[derive(Clone)]
struct VlessUdpFlowSender {
    send_tx: mpsc::Sender<VlessUdpFlowSend>,
}

#[cfg(feature = "reality")]
struct VlessUdpFlowHandle {
    sender: VlessUdpFlowSender,
    responses: VlessUdpFlowResponses,
}

#[cfg(feature = "reality")]
#[derive(Clone)]
struct VlessUdpFlowSession {
    sender: VlessUdpFlowSender,
    responses: VlessUdpFlowResponses,
}

#[cfg(feature = "reality")]
impl VlessUdpFlowSession {
    fn new(handle: VlessUdpFlowHandle) -> Self {
        Self {
            sender: handle.sender,
            responses: handle.responses,
        }
    }

    async fn send(&self, target: &Address, port: u16, payload: &[u8]) -> Result<usize, Error> {
        self.sender.send(target, port, payload).await
    }

    fn subscribe_responses(&self) -> VlessUdpFlowResponseReceiver {
        self.responses.subscribe()
    }
}

#[cfg(feature = "reality")]
#[derive(Clone)]
pub struct VlessUdpFlowConnection {
    session: VlessUdpFlowSession,
}

#[cfg(feature = "reality")]
impl VlessUdpFlowConnection {
    fn new(handle: VlessUdpFlowHandle) -> Self {
        Self {
            session: VlessUdpFlowSession::new(handle),
        }
    }

    pub async fn send(&self, target: &Address, port: u16, payload: &[u8]) -> Result<usize, Error> {
        self.session.send(target, port, payload).await
    }

    pub fn subscribe_responses(&self) -> VlessUdpFlowResponseReceiver {
        self.session.subscribe_responses()
    }
}

#[cfg(feature = "reality")]
impl VlessUdpFlowSender {
    async fn send(&self, target: &Address, port: u16, payload: &[u8]) -> Result<usize, Error> {
        let packet = zero_core::UdpFlowPacket::from_parts(target, port, payload);
        let (result_tx, result_rx) = oneshot::channel();
        self.send_tx
            .send(VlessUdpFlowSend { packet, result_tx })
            .await
            .map_err(|_| Error::Io("vless udp flow closed"))?;
        result_rx
            .await
            .map_err(|_| Error::Io("vless udp flow closed"))?
    }
}

#[cfg(feature = "reality")]
impl VlessEstablishedUdpFlow {
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
        if target != &self.target || port != self.port {
            return Err(Error::Protocol(
                "VLESS UDP packet target differs from the established flow",
            ));
        }
        write_plain_udp_packet(stream, payload).await
    }

    async fn read_packet_tokio<S>(
        &self,
        stream: &mut S,
        response_pending: &mut bool,
    ) -> Result<Option<VlessUdpFlowPacket>, Error>
    where
        S: tokio::io::AsyncRead + Unpin,
    {
        if *response_pending {
            read_vless_response_tokio(stream).await?;
            *response_pending = false;
        }
        read_plain_udp_packet(stream, &self.target, self.port).await
    }
}

#[cfg(feature = "reality")]
fn spawn_udp_flow<S>(stream: S, flow_io: VlessEstablishedUdpFlow) -> VlessUdpFlowHandle
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
{
    let (send_tx, send_rx) = mpsc::channel::<VlessUdpFlowSend>(32);
    let (responses, _) = broadcast::channel::<VlessUdpFlowResponse>(32);
    spawn_udp_flow_task(stream, send_rx, responses.clone(), flow_io);
    VlessUdpFlowHandle {
        sender: VlessUdpFlowSender { send_tx },
        responses,
    }
}

#[cfg(feature = "reality")]
pub(crate) fn start_mux_udp_flow(
    up_tx: mpsc::UnboundedSender<zero_core::UdpFlowPacket>,
    down_rx: mpsc::Receiver<crate::mux_pool::MuxDownlink<zero_core::UdpFlowPacket>>,
) -> VlessUdpFlowConnection {
    let (send_tx, send_rx) = mpsc::channel::<VlessUdpFlowSend>(32);
    let (responses, _) = broadcast::channel::<VlessUdpFlowResponse>(32);
    spawn_mux_udp_flow_task(send_rx, up_tx, down_rx, responses.clone());
    VlessUdpFlowConnection::new(VlessUdpFlowHandle {
        sender: VlessUdpFlowSender { send_tx },
        responses,
    })
}

#[cfg(feature = "reality")]
async fn establish_udp_flow_with_resume<S>(
    mut stream: S,
    session: &Session,
    resume: &VlessUdpFlowResume,
) -> Result<VlessUdpFlowConnection, Error>
where
    S: AsyncSocket + tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + 'static,
{
    let flow_io = establish_udp_flow(&mut stream, session, resume.identity()).await?;
    Ok(VlessUdpFlowConnection::new(spawn_udp_flow(stream, flow_io)))
}

#[cfg(feature = "reality")]
fn spawn_udp_flow_task<S>(
    mut stream: S,
    mut send_rx: mpsc::Receiver<VlessUdpFlowSend>,
    responses: VlessUdpFlowResponses,
    flow_io: VlessEstablishedUdpFlow,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
{
    tokio::spawn(async move {
        let mut response_pending = true;
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
                read = flow_io.read_packet_tokio(&mut stream, &mut response_pending) => {
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

#[cfg(feature = "reality")]
fn spawn_mux_udp_flow_task(
    mut send_rx: mpsc::Receiver<VlessUdpFlowSend>,
    up_tx: mpsc::UnboundedSender<zero_core::UdpFlowPacket>,
    mut down_rx: mpsc::Receiver<crate::mux_pool::MuxDownlink<zero_core::UdpFlowPacket>>,
    responses: VlessUdpFlowResponses,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                to_send = send_rx.recv() => {
                    match to_send {
                        Some(request) => {
                            let payload_len = request.packet.payload.len();
                            let result = up_tx
                                .send(request.packet)
                                .map(|_| payload_len)
                                .map_err(|_| Error::Io("vless mux udp flow closed"));
                            let should_break = result.is_err();
                            let _ = request.result_tx.send(result);
                            if should_break {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                read = down_rx.recv() => {
                    match read {
                        Some(crate::mux_pool::MuxDownlink::Data(packet)) => {
                            let packet = packet.into_inner();
                            let _ = responses.send(packet.into_parts());
                        }
                        Some(crate::mux_pool::MuxDownlink::Overflow) => break,
                        None => break,
                    }
                }
            }
        }
    });
}

#[cfg(feature = "reality")]
async fn establish_udp_flow<S>(
    stream: &mut S,
    session: &Session,
    identity: VlessUdpIdentity,
) -> Result<VlessEstablishedUdpFlow, Error>
where
    S: AsyncSocket,
{
    establish_udp_flow_stream(stream, session, identity).await?;
    Ok(VlessEstablishedUdpFlow {
        target: session.target.clone(),
        port: session.port,
    })
}

#[cfg(feature = "reality")]
async fn write_plain_udp_packet<S>(stream: &mut S, payload: &[u8]) -> Result<usize, Error>
where
    S: tokio::io::AsyncWrite + Unpin,
{
    let payload_len = u16::try_from(payload.len())
        .map_err(|_| Error::Protocol("VLESS UDP payload is too large"))?;
    tokio::io::AsyncWriteExt::write_all(stream, &payload_len.to_be_bytes())
        .await
        .map_err(|_| Error::Io("vless udp flow write length"))?;
    tokio::io::AsyncWriteExt::write_all(stream, payload)
        .await
        .map_err(|_| Error::Io("vless udp flow write payload"))?;
    tokio::io::AsyncWriteExt::flush(stream)
        .await
        .map_err(|_| Error::Io("vless udp flow flush"))?;
    Ok(2 + payload.len())
}

#[cfg(feature = "reality")]
pub(crate) async fn read_vless_response_tokio<S>(stream: &mut S) -> Result<(), Error>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut header = [0_u8; 2];
    tokio::io::AsyncReadExt::read_exact(stream, &mut header)
        .await
        .map_err(|_| Error::Io("vless udp flow read response"))?;
    if header[0] != crate::VLESS_VERSION {
        return Err(Error::Protocol("unsupported VLESS response version"));
    }
    if header[1] != 0 {
        let mut addon = vec![0_u8; header[1] as usize];
        tokio::io::AsyncReadExt::read_exact(stream, &mut addon)
            .await
            .map_err(|_| Error::Io("vless udp flow read response addon"))?;
    }
    Ok(())
}

#[cfg(feature = "reality")]
async fn read_plain_udp_packet<S>(
    stream: &mut S,
    target: &Address,
    port: u16,
) -> Result<Option<VlessUdpFlowPacket>, Error>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut length = [0_u8; 2];
    match tokio::io::AsyncReadExt::read_exact(stream, &mut length).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(_) => return Err(Error::Io("vless udp flow read length")),
    }
    let mut payload = vec![0_u8; u16::from_be_bytes(length) as usize];
    tokio::io::AsyncReadExt::read_exact(stream, &mut payload)
        .await
        .map_err(|_| Error::Io("vless udp flow read payload"))?;
    Ok(Some(VlessUdpFlowPacket::new(target.clone(), port, payload)))
}

impl<'a> UdpPacketTunnelProtocol<VlessUdpPacketTunnelTarget<'a>> for VlessOutbound {
    type Error = Error;

    async fn establish_udp_packet_tunnel<S>(
        &self,
        stream: &mut S,
        target: &VlessUdpPacketTunnelTarget<'a>,
    ) -> Result<(), Self::Error>
    where
        S: AsyncSocket,
    {
        establish_udp_packet_tunnel(stream, target.session, target.id).await
    }
}

fn build_udp_request(session: &Session, id: &[u8; 16]) -> Result<Vec<u8>, Error> {
    crate::shared::build_request(session, id, CMD_UDP)
}

pub async fn send_udp_request<S>(
    stream: &mut S,
    session: &Session,
    id: &[u8; 16],
) -> Result<(), Error>
where
    S: AsyncSocket,
{
    if session.port == 0 {
        return Err(Error::Config("target port is required"));
    }

    let request = build_udp_request(session, id)?;
    stream
        .write_all(&request)
        .await
        .map_err(|_| Error::Io("failed to write VLESS UDP request"))
}

pub async fn establish_udp_packet_tunnel<S>(
    stream: &mut S,
    session: &Session,
    id: &[u8; 16],
) -> Result<(), Error>
where
    S: AsyncSocket,
{
    send_udp_request(stream, session, id).await?;
    read_response(stream).await
}

/// One UDP datagram to encode for a VLESS UDP packet tunnel.
#[derive(Debug, Clone, Copy)]
pub struct VlessUdpPacketTarget<'a> {
    pub address: &'a Address,
    pub port: u16,
    pub payload: &'a [u8],
}

impl<'a> UdpPacketFraming<VlessUdpPacketTarget<'a>> for VlessOutbound {
    type Error = Error;
    type Decoded = VlessUdpPacket;

    fn encode_udp_packet(&self, packet: &VlessUdpPacketTarget<'a>) -> Result<Vec<u8>, Self::Error> {
        build_udp_packet(packet.address, packet.port, packet.payload)
    }

    fn decode_udp_packet(&self, packet: &[u8]) -> Result<Self::Decoded, Self::Error> {
        parse_udp_packet(packet)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VlessUdpPacket {
    target: Address,
    port: u16,
    payload: Vec<u8>,
}

impl VlessUdpPacket {
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
struct VlessInboundUdpRequest {
    target: Address,
    port: u16,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VlessInboundUdpDispatchParts {
    target: Address,
    port: u16,
    payload: Vec<u8>,
    client_session_id: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct VlessInboundUdpClientResponse<'a> {
    target: &'a Address,
    port: u16,
    payload: &'a [u8],
}

impl<'a> VlessInboundUdpClientResponse<'a> {
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

impl VlessInboundUdpRequest {
    fn from_packet(packet: VlessUdpPacket) -> Self {
        let (target, port, payload) = packet.into_parts();
        Self {
            target,
            port,
            payload,
        }
    }

    pub fn into_parts(self) -> (Address, u16, Vec<u8>) {
        (self.target, self.port, self.payload)
    }

    pub fn into_dispatch_parts(self) -> VlessInboundUdpDispatchParts {
        let (target, port, payload) = self.into_parts();
        VlessInboundUdpDispatchParts {
            target,
            port,
            payload,
            client_session_id: None,
        }
    }
}

impl VlessInboundUdpDispatchParts {
    pub fn protocol(&self) -> ProtocolType {
        ProtocolType::new("vless")
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VlessUdpFlowPacket {
    target: Address,
    port: u16,
    payload: Vec<u8>,
}

impl VlessUdpFlowPacket {
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

#[cfg(feature = "reality")]
pub fn encode_udp_flow_initial_packet(
    target: &Address,
    port: u16,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    VlessUdpFlowIo.encode_packet(target, port, payload)
}

#[derive(Debug, Clone, Copy, Default)]
struct VlessUdpFlowIo;

impl VlessUdpFlowIo {
    fn encode_packet(&self, target: &Address, port: u16, payload: &[u8]) -> Result<Vec<u8>, Error> {
        encode_udp_flow_packet(target, port, payload)
    }
}

pub(crate) fn parse_udp_packet(packet: &[u8]) -> Result<VlessUdpPacket, Error> {
    if packet.len() < 3 {
        return Err(Error::Protocol("VLESS UDP packet is too short"));
    }

    let mut offset = 0;
    let port = u16::from_be_bytes([packet[offset], packet[offset + 1]]);
    offset += 2;

    let atyp = packet[offset];
    offset += 1;

    let target = match atyp {
        ATYP_IPV4 => {
            if packet.len() < offset + 4 {
                return Err(Error::Protocol("VLESS UDP IPv4 packet is truncated"));
            }
            let mut bytes = [0_u8; 4];
            bytes.copy_from_slice(&packet[offset..offset + 4]);
            offset += 4;
            Address::Ipv4(bytes)
        }
        ATYP_IPV6 => {
            if packet.len() < offset + 16 {
                return Err(Error::Protocol("VLESS UDP IPv6 packet is truncated"));
            }
            let mut bytes = [0_u8; 16];
            bytes.copy_from_slice(&packet[offset..offset + 16]);
            offset += 16;
            Address::Ipv6(bytes)
        }
        ATYP_DOMAIN => {
            if packet.len() < offset + 1 {
                return Err(Error::Protocol("VLESS UDP domain packet is truncated"));
            }
            let len = packet[offset] as usize;
            offset += 1;
            if len == 0 || packet.len() < offset + len {
                return Err(Error::Protocol("VLESS UDP domain packet is truncated"));
            }
            let domain = String::from_utf8(packet[offset..offset + len].to_vec())
                .map_err(|_| Error::Protocol("VLESS UDP domain is not valid UTF-8"))?;
            offset += len;
            Address::Domain(domain)
        }
        _ => {
            return Err(Error::Unsupported(
                "VLESS UDP address type is not supported",
            ));
        }
    };

    Ok(VlessUdpPacket {
        target,
        port,
        payload: packet[offset..].to_vec(),
    })
}

pub(crate) fn build_udp_packet(
    address: &Address,
    port: u16,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    let mut packet = Vec::with_capacity(2 + 1 + payload.len());
    packet.extend_from_slice(&port.to_be_bytes());
    write_address(&mut packet, address)?;
    packet.extend_from_slice(payload);
    Ok(packet)
}

const UDP_V2_HAS_ADDR: u8 = 0x01;
const UDP_V2_MARKER: [u8; 2] = [0x00, 0x00];

pub(crate) fn parse_udp_packet_v2(
    packet: &[u8],
    cached_target: Option<&Address>,
    cached_port: Option<u16>,
) -> Result<VlessUdpPacket, Error> {
    if packet.len() < 3 {
        return Err(Error::Protocol("VLESS UDP packet is too short"));
    }

    if packet[0] == UDP_V2_MARKER[0] && packet[1] == UDP_V2_MARKER[1] {
        parse_udp_v2(packet, cached_target, cached_port)
    } else {
        parse_udp_packet(packet)
    }
}

fn parse_udp_v2(
    packet: &[u8],
    cached_target: Option<&Address>,
    cached_port: Option<u16>,
) -> Result<VlessUdpPacket, Error> {
    let flags = packet[2];
    let has_addr = flags & UDP_V2_HAS_ADDR != 0;

    if has_addr {
        if packet.len() < 8 {
            return Err(Error::Protocol("VLESS UDP v2 packet is too short"));
        }
        let port = u16::from_be_bytes([packet[3], packet[4]]);
        let atyp = packet[5];
        let (target, addr_len) = parse_addr_from_packet(atyp, &packet[6..])?;
        let payload = packet[6 + addr_len..].to_vec();
        Ok(VlessUdpPacket {
            target,
            port,
            payload,
        })
    } else {
        let target = cached_target
            .ok_or(Error::Protocol("VLESS UDP v2: no cached target"))?
            .clone();
        let port = cached_port.ok_or(Error::Protocol("VLESS UDP v2: no cached port"))?;
        Ok(VlessUdpPacket {
            target,
            port,
            payload: packet[3..].to_vec(),
        })
    }
}

fn parse_addr_from_packet(atyp: u8, data: &[u8]) -> Result<(Address, usize), Error> {
    match atyp {
        ATYP_IPV4 => {
            if data.len() < 4 {
                return Err(Error::Protocol("VLESS UDP v2 IPv4 address is truncated"));
            }
            let mut bytes = [0_u8; 4];
            bytes.copy_from_slice(&data[..4]);
            Ok((Address::Ipv4(bytes), 4))
        }
        ATYP_IPV6 => {
            if data.len() < 16 {
                return Err(Error::Protocol("VLESS UDP v2 IPv6 address is truncated"));
            }
            let mut bytes = [0_u8; 16];
            bytes.copy_from_slice(&data[..16]);
            Ok((Address::Ipv6(bytes), 16))
        }
        ATYP_DOMAIN => {
            if data.is_empty() {
                return Err(Error::Protocol("VLESS UDP v2 domain packet is truncated"));
            }
            let len = data[0] as usize;
            if len == 0 || data.len() < 1 + len {
                return Err(Error::Protocol("VLESS UDP v2 domain packet is truncated"));
            }
            let domain = String::from_utf8(data[1..1 + len].to_vec())
                .map_err(|_| Error::Protocol("VLESS UDP v2 domain is not valid UTF-8"))?;
            Ok((Address::Domain(domain), 1 + len))
        }
        _ => Err(Error::Unsupported(
            "VLESS UDP v2 address type is not supported",
        )),
    }
}

pub(crate) fn build_udp_packet_v2(
    address: &Address,
    port: u16,
    payload: &[u8],
    omit_address: bool,
) -> Result<Vec<u8>, Error> {
    if omit_address {
        let mut packet = Vec::with_capacity(3 + payload.len());
        packet.extend_from_slice(&UDP_V2_MARKER);
        packet.push(0x00);
        packet.extend_from_slice(payload);
        Ok(packet)
    } else {
        let mut packet = Vec::with_capacity(6 + 1 + payload.len());
        packet.extend_from_slice(&UDP_V2_MARKER);
        packet.push(UDP_V2_HAS_ADDR);
        packet.extend_from_slice(&port.to_be_bytes());
        write_address(&mut packet, address)?;
        packet.extend_from_slice(payload);
        Ok(packet)
    }
}

pub(crate) fn decode_inbound_udp_packet(packet: &[u8]) -> Result<VlessUdpPacket, Error> {
    parse_udp_packet(packet)
}

pub(crate) fn encode_udp_response(
    target: &Address,
    port: u16,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    build_udp_packet(target, port, payload)
}

fn decode_inbound_udp_datagram(packet: &[u8]) -> Result<VlessUdpPacket, Error> {
    decode_inbound_udp_packet(packet)
}

fn encode_inbound_udp_response(
    target: &Address,
    port: u16,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    encode_udp_response(target, port, payload)
}

fn encode_inbound_mux_udp_response(
    mux_session_id: u16,
    target: &Address,
    port: u16,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    encode_mux_udp_response(mux_session_id, target, port, payload)
}

#[derive(Debug, Default, Clone, Copy)]
struct VlessInboundUdpCodec;

impl VlessInboundUdpCodec {
    fn decode_request(&self, packet: &[u8]) -> Result<VlessInboundUdpRequest, Error> {
        self.decode_datagram(packet)
            .map(VlessInboundUdpRequest::from_packet)
    }

    fn decode_dispatch_parts(&self, packet: &[u8]) -> Result<VlessInboundUdpDispatchParts, Error> {
        self.decode_request(packet)
            .map(VlessInboundUdpRequest::into_dispatch_parts)
    }

    fn decode_datagram(&self, packet: &[u8]) -> Result<VlessUdpPacket, Error> {
        decode_inbound_udp_datagram(packet)
    }

    fn encode_response(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Vec<u8>, Error> {
        encode_inbound_udp_response(target, port, payload)
    }

    fn encode_mux_response(
        &self,
        mux_session_id: u16,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Vec<u8>, Error> {
        encode_inbound_mux_udp_response(mux_session_id, target, port, payload)
    }

    #[cfg(feature = "reality")]
    fn send_mux_response(
        &self,
        writer: &crate::mux::VlessInboundMuxWriter,
        mux_session_id: u16,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error> {
        writer.udp(mux_session_id, target, port, payload)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct VlessInboundUdpSession {
    codec: VlessInboundUdpCodec,
}

#[cfg(feature = "reality")]
pub struct VlessInboundUdpResponder {
    target: Address,
    port: u16,
}

#[cfg(feature = "reality")]
pub(crate) struct VlessInboundMuxUdpResponder {
    session: VlessInboundUdpSession,
    writer: crate::mux::VlessInboundMuxWriter,
    mux_session_id: u16,
}

impl VlessInboundUdpSession {
    pub(crate) fn new() -> Self {
        Self {
            codec: VlessInboundUdpCodec,
        }
    }

    fn decode_dispatch_parts(&self, packet: &[u8]) -> Result<VlessInboundUdpDispatchParts, Error> {
        self.codec.decode_dispatch_parts(packet)
    }

    fn decode_inbound_dispatch(&self, packet: &[u8]) -> Result<InboundUdpDispatch, Error> {
        self.decode_dispatch_parts(packet)
            .map(VlessInboundUdpDispatchParts::into_inbound_dispatch)
    }

    fn decode_mux_inbound_dispatch(&self, payload: &[u8]) -> Result<InboundUdpDispatch, Error> {
        self.decode_inbound_dispatch(payload)
    }

    fn encode_response_packet(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Vec<u8>, Error> {
        self.codec.encode_response(target, port, payload)
    }

    #[cfg(feature = "reality")]
    pub(crate) fn send_mux_response(
        &self,
        writer: &crate::mux::VlessInboundMuxWriter,
        mux_session_id: u16,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error> {
        self.codec
            .send_mux_response(writer, mux_session_id, target, port, payload)
    }

    #[cfg(feature = "reality")]
    fn send_mux_client_response(
        &self,
        writer: &crate::mux::VlessInboundMuxWriter,
        mux_session_id: u16,
        response: VlessInboundUdpClientResponse<'_>,
    ) -> Result<usize, Error> {
        self.send_mux_response(
            writer,
            mux_session_id,
            response.target(),
            response.port(),
            response.payload(),
        )
    }

    #[cfg(feature = "reality")]
    fn send_mux_client_response_for_target(
        &self,
        writer: &crate::mux::VlessInboundMuxWriter,
        mux_session_id: u16,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error> {
        self.send_mux_client_response(
            writer,
            mux_session_id,
            VlessInboundUdpClientResponse::new(target, port, payload),
        )
    }

    #[cfg(feature = "reality")]
    fn encode_mux_response_packet(
        &self,
        mux_session_id: u16,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Vec<u8>, Error> {
        self.codec
            .encode_mux_response(mux_session_id, target, port, payload)
    }
}

pub fn decode_inbound_dispatch(packet: &[u8]) -> Result<InboundUdpDispatch, Error> {
    VlessInboundUdpSession::new().decode_inbound_dispatch(packet)
}

pub fn decode_mux_inbound_dispatch(payload: &[u8]) -> Result<InboundUdpDispatch, Error> {
    VlessInboundUdpSession::new().decode_mux_inbound_dispatch(payload)
}

pub fn encode_response_packet(
    target: &Address,
    port: u16,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    VlessInboundUdpSession::new().encode_response_packet(target, port, payload)
}

#[cfg(feature = "reality")]
pub fn encode_mux_response_packet(
    mux_session_id: u16,
    target: &Address,
    port: u16,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    VlessInboundUdpSession::new().encode_mux_response_packet(mux_session_id, target, port, payload)
}

#[cfg(feature = "reality")]
impl VlessInboundUdpResponder {
    pub(crate) fn new(target: Address, port: u16) -> Self {
        Self { target, port }
    }

    async fn read_inbound_dispatch_tokio<R>(
        &mut self,
        reader: &mut R,
    ) -> Result<Option<InboundUdpDispatch>, Error>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        let Some(packet) = read_plain_udp_packet(reader, &self.target, self.port).await? else {
            return Ok(None);
        };
        let (target, port, payload) = packet.into_parts();
        Ok(Some(InboundUdpDispatch::new(
            ProtocolType::new("vless"),
            target,
            port,
            payload,
            None,
        )))
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
        if target != &self.target || port != self.port {
            return Err(Error::Protocol(
                "VLESS UDP response target differs from the established flow",
            ));
        }
        write_plain_udp_packet(writer, payload).await
    }
}

#[cfg(feature = "reality")]
#[async_trait::async_trait]
impl<S> StreamUdpResponder<S> for VlessInboundUdpResponder
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

#[cfg(feature = "reality")]
impl VlessInboundMuxUdpResponder {
    pub(crate) fn new(
        session: VlessInboundUdpSession,
        writer: crate::mux::VlessInboundMuxWriter,
        mux_session_id: u16,
    ) -> Self {
        Self {
            session,
            writer,
            mux_session_id,
        }
    }

    pub(crate) fn decode_inbound_dispatch(
        &self,
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
        self.session.send_mux_client_response_for_target(
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

#[cfg(feature = "reality")]
impl MuxUdpResponder for VlessInboundMuxUdpResponder {
    fn decode_inbound_dispatch(&mut self, payload: &[u8]) -> Result<InboundUdpDispatch, Error> {
        VlessInboundMuxUdpResponder::decode_inbound_dispatch(self, payload)
    }

    fn write_response_for_target(
        &mut self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error> {
        VlessInboundMuxUdpResponder::write_response_for_target(self, target, port, payload)
    }

    fn end_inbound_stream(&mut self) -> Result<usize, Error> {
        VlessInboundMuxUdpResponder::end_inbound_stream(self)
    }

    fn decode_failure(&self) -> MuxUdpDecodeFailure {
        MuxUdpDecodeFailure::Continue
    }
}

pub(crate) fn encode_udp_flow_packet(
    target: &Address,
    port: u16,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    build_udp_packet(target, port, payload)
}

pub(crate) fn encode_mux_udp_response(
    mux_session_id: u16,
    target: &Address,
    port: u16,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    crate::mux::encode_udp_data_frame(mux_session_id, target, port, payload)
}

#[derive(Debug, Default, Clone, Copy)]
pub struct VlessUdpPacketV2Codec;

impl VlessUdpPacketV2Codec {
    pub fn decode_packet(
        &self,
        packet: &[u8],
        cached_target: Option<&Address>,
        cached_port: Option<u16>,
    ) -> Result<VlessUdpPacket, Error> {
        parse_udp_packet_v2(packet, cached_target, cached_port)
    }

    pub fn encode_packet(
        &self,
        address: &Address,
        port: u16,
        payload: &[u8],
        omit_address: bool,
    ) -> Result<Vec<u8>, Error> {
        build_udp_packet_v2(address, port, payload, omit_address)
    }
}

impl crate::inbound::VlessInbound {
    #[cfg(feature = "reality")]
    pub(crate) fn udp_responder(&self, session: &Session) -> VlessInboundUdpResponder {
        VlessInboundUdpResponder::new(session.target.clone(), session.port)
    }

    #[cfg(feature = "reality")]
    pub(crate) async fn accept_udp_session<S>(
        &self,
        stream: &mut S,
        session: &Session,
    ) -> Result<VlessInboundUdpResponder, Error>
    where
        S: AsyncSocket,
    {
        self.send_response(stream).await?;
        Ok(self.udp_responder(session))
    }
}
