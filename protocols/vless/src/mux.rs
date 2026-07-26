// VLESS MUX (Connection Multiplexing) — mux.rs
//
// Encodes multiple TCP/UDP streams within a single VLESS connection.
//
// Native Mux.Cool frame:
//   [metadata length: u16]
//   [session ID: u16][status: u8][options: u8][optional target/global ID]
//   [data length: u16][data...] when OPTION_DATA is set
//
// The metadata length never includes the independent data-length field or data.
//
// Status codes:
//   0x01 StatusNew      — New connection request
//   0x02 StatusKeep     — Ongoing session data
//   0x03 StatusEnd      — Session termination
//   0x04 StatusKeepAlive — Keep-alive signal
//
// A generic MUX client owns its non-zero sub-session IDs. The single-session
// XUDP form may use ID 0. Servers do not assign IDs or send an acceptance frame.

use alloc::boxed::Box;
use alloc::vec::Vec;

#[cfg(feature = "reality")]
use tokio::sync::mpsc;
use zero_core::{Address, Error, Network, ProtocolType, Session};
#[cfg(feature = "reality")]
use zero_core::{
    InboundMuxTcpRelay, InboundMuxUdpReadFailure, InboundMuxUdpReadFailureAction,
    InboundMuxUdpRelay, MuxUdpDecodeFailure, MuxUdpResponder, SessionAuth,
};
use zero_traits::AsyncSocket;

use crate::shared::{ATYP_DOMAIN, ATYP_IPV4, ATYP_IPV6};

pub(crate) mod backlog;
mod codec;
#[cfg(all(test, feature = "reality"))]
mod tests;

pub(crate) use backlog::MuxResponseBacklogPolicy;
#[cfg(feature = "reality")]
use backlog::{BufferedMuxResponse, MuxResponseBacklog};

// ── Constants ──

pub const MUX_MAX_METADATA: usize = 512;
pub const MUX_MAX_PAYLOAD: usize = 8192;

// Status codes
pub const STATUS_NEW: u8 = 0x01;
pub const STATUS_KEEP: u8 = 0x02;
pub const STATUS_END: u8 = 0x03;
pub const STATUS_KEEP_ALIVE: u8 = 0x04;

// Option flags
pub const OPTION_DATA: u8 = 0x01;
pub const OPTION_ERROR: u8 = 0x02;

// Network types
pub const NETWORK_TCP: u8 = 0x01;
pub const NETWORK_UDP: u8 = 0x02;

// Backward-compat aliases for network type constants
pub const MUX_NETWORK_TCP: u8 = NETWORK_TCP;
pub const MUX_NETWORK_UDP: u8 = NETWORK_UDP;

// ── Types ──

/// Parsed Mux.Cool frame.
#[derive(Debug, Clone)]
pub(crate) struct MuxFrame {
    pub session_id: u16,
    pub status: u8,
    pub options: u8,
    pub target: Option<MuxTarget>,
    pub global_id: Option<[u8; 8]>,
    pub payload: Vec<u8>,
}

/// Target info for a new MUX stream.
#[derive(Debug, Clone)]
pub(crate) struct MuxTarget {
    pub network: u8,
    pub port: u16,
    pub address: Address,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MuxNetwork {
    Tcp,
    Udp,
}

impl MuxTarget {
    fn network_kind(&self) -> Result<MuxNetwork, Error> {
        match self.network {
            NETWORK_TCP => Ok(MuxNetwork::Tcp),
            NETWORK_UDP => Ok(MuxNetwork::Udp),
            _ => Err(Error::Protocol("MUX new stream unknown network type")),
        }
    }

    fn into_session(self) -> Result<Session, Error> {
        let network = match self.network_kind()? {
            MuxNetwork::Tcp => Network::Tcp,
            MuxNetwork::Udp => Network::Udp,
        };
        Ok(Session::new(
            0,
            self.address,
            self.port,
            network,
            ProtocolType::new("vless"),
        ))
    }
}

#[derive(Debug, Clone)]
enum MuxServerEvent {
    KeepAlive,
    NewStream {
        session_id: u16,
        target: MuxTarget,
        global_id: Option<[u8; 8]>,
        initial_payload: Vec<u8>,
    },
    Data {
        session_id: u16,
        target: Option<MuxTarget>,
        payload: Vec<u8>,
    },
    End {
        session_id: u16,
    },
    Unknown {
        session_id: u16,
    },
}

#[derive(Debug, Clone)]
enum VlessInboundMuxAction {
    KeepAlive,
    OpenStream {
        session_id: u16,
        session: Box<Session>,
        global_id: Option<[u8; 8]>,
        initial_payload: Vec<u8>,
    },
    Data {
        session_id: u16,
        target: Option<MuxTarget>,
        payload: Vec<u8>,
    },
    End {
        session_id: u16,
    },
    Unknown {
        session_id: u16,
    },
}

#[cfg(feature = "reality")]
struct VlessInboundMuxOpenedStream {
    session_id: u16,
    session: Box<Session>,
    global_id: Option<[u8; 8]>,
    termination_probe: zero_core::InboundMuxUdpTerminationProbe,
    up_rx: mpsc::UnboundedReceiver<Vec<u8>>,
}

#[cfg(feature = "reality")]
enum VlessInboundMuxOpenedRouteState {
    Tcp {
        session: Box<Session>,
        relay: VlessInboundMuxTcpRelay,
    },
    Udp {
        relay: VlessInboundMuxUdpRelay,
    },
}

#[cfg(feature = "reality")]
struct VlessInboundMuxOpenedRoute {
    state: VlessInboundMuxOpenedRouteState,
}

#[cfg(feature = "reality")]
impl VlessInboundMuxOpenedRoute {
    fn tcp(session: Session, relay: VlessInboundMuxTcpRelay) -> Self {
        Self {
            state: VlessInboundMuxOpenedRouteState::Tcp {
                session: Box::new(session),
                relay,
            },
        }
    }

    fn udp(relay: VlessInboundMuxUdpRelay) -> Self {
        Self {
            state: VlessInboundMuxOpenedRouteState::Udp { relay },
        }
    }
}

#[cfg(feature = "reality")]
pub struct VlessInboundMuxTcpRelay {
    session_id: u16,
    up_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    writer: VlessInboundMuxWriter,
}

#[cfg(feature = "reality")]
impl VlessInboundMuxTcpRelay {
    fn new(
        session_id: u16,
        up_rx: mpsc::UnboundedReceiver<Vec<u8>>,
        writer: VlessInboundMuxWriter,
    ) -> Self {
        Self {
            session_id,
            up_rx,
            writer,
        }
    }

    async fn relay_stream<S>(self, upstream: S)
    where
        S: AsyncSocket + 'static,
        S::Error: Send,
    {
        relay_inbound_mux_stream(self.session_id, self.up_rx, self.writer, upstream).await;
    }
}

#[cfg(feature = "reality")]
impl InboundMuxTcpRelay for VlessInboundMuxTcpRelay {
    fn mux_session_id(&self) -> u16 {
        self.session_id
    }

    fn close_stream(&self) -> impl core::future::Future<Output = ()> + Send {
        let session_id = self.session_id;
        let writer = self.writer.clone();
        async move {
            let _ = writer.end_inbound_stream(session_id);
        }
    }

    async fn relay_stream<S>(self, upstream: S)
    where
        S: AsyncSocket + 'static,
        S::Error: Send,
    {
        VlessInboundMuxTcpRelay::relay_stream(self, upstream).await;
    }
}

#[cfg(feature = "reality")]
impl VlessInboundMuxOpenedStream {
    fn new(
        session_id: u16,
        session: Box<Session>,
        global_id: Option<[u8; 8]>,
        termination_probe: zero_core::InboundMuxUdpTerminationProbe,
        up_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    ) -> Self {
        Self {
            session_id,
            session,
            global_id,
            termination_probe,
            up_rx,
        }
    }

    fn into_parts(
        self,
    ) -> (
        u16,
        Session,
        Option<[u8; 8]>,
        zero_core::InboundMuxUdpTerminationProbe,
        mpsc::UnboundedReceiver<Vec<u8>>,
    ) {
        (
            self.session_id,
            *self.session,
            self.global_id,
            self.termination_probe,
            self.up_rx,
        )
    }

    fn into_route_with_auth(
        self,
        auth: Option<&SessionAuth>,
        writer: VlessInboundMuxWriter,
    ) -> VlessInboundMuxOpenedRoute {
        let (session_id, mut session, global_id, termination_probe, up_rx) = self.into_parts();
        if let Some(auth) = auth {
            session.apply_auth(auth.clone());
        }
        match session.network {
            Network::Tcp => VlessInboundMuxOpenedRoute::tcp(
                session,
                VlessInboundMuxTcpRelay::new(session_id, up_rx, writer),
            ),
            Network::Udp => VlessInboundMuxOpenedRoute::udp(VlessInboundMuxUdpRelay::new(
                session_id,
                up_rx,
                crate::udp::VlessInboundMuxUdpResponder::new(
                    crate::udp::VlessInboundUdpSession::new(),
                    writer,
                    session_id,
                ),
                auth.cloned(),
                global_id
                    .filter(|global_id| *global_id != [0; 8])
                    .and_then(|global_id| zero_core::UdpContinuityKey::from_bytes(&global_id)),
                termination_probe,
            )),
        }
    }
}

#[cfg(feature = "reality")]
pub struct VlessInboundMuxUdpRelay {
    session_id: u16,
    up_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    responder: crate::udp::VlessInboundMuxUdpResponder,
    auth: Option<SessionAuth>,
    continuity_key: Option<zero_core::UdpContinuityKey>,
    termination_probe: zero_core::InboundMuxUdpTerminationProbe,
}

#[cfg(feature = "reality")]
impl VlessInboundMuxUdpRelay {
    fn new(
        session_id: u16,
        up_rx: mpsc::UnboundedReceiver<Vec<u8>>,
        responder: crate::udp::VlessInboundMuxUdpResponder,
        auth: Option<SessionAuth>,
        continuity_key: Option<zero_core::UdpContinuityKey>,
        termination_probe: zero_core::InboundMuxUdpTerminationProbe,
    ) -> Self {
        Self {
            session_id,
            up_rx,
            responder,
            auth,
            continuity_key,
            termination_probe,
        }
    }
}

#[cfg(feature = "reality")]
#[async_trait::async_trait]
impl InboundMuxUdpRelay for VlessInboundMuxUdpRelay {
    async fn read_inbound_dispatch(
        &mut self,
    ) -> Result<Option<zero_core::InboundUdpDispatch>, InboundMuxUdpReadFailure> {
        let Some(payload) = self.up_rx.recv().await else {
            return Ok(None);
        };
        if payload.is_empty() {
            return Ok(None);
        }

        match self.responder.decode_inbound_dispatch(&payload) {
            Ok(inbound_dispatch) => Ok(Some(inbound_dispatch)),
            Err(error) => Err(InboundMuxUdpReadFailure {
                error,
                action: match self.responder.decode_failure() {
                    MuxUdpDecodeFailure::Continue => InboundMuxUdpReadFailureAction::Continue,
                    MuxUdpDecodeFailure::End => InboundMuxUdpReadFailureAction::End,
                },
            }),
        }
    }

    fn write_response_for_target(
        &mut self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error> {
        self.responder
            .write_response_for_target(target, port, payload)
    }

    fn end_inbound_stream(&mut self) -> Result<usize, Error> {
        self.responder.end_inbound_stream()
    }

    fn mux_session_id(&self) -> u16 {
        self.session_id
    }

    fn continuity_key(&self) -> Option<&zero_core::UdpContinuityKey> {
        self.continuity_key.as_ref()
    }

    fn termination_probe(&self) -> Option<zero_core::InboundMuxUdpTerminationProbe> {
        Some(self.termination_probe.clone())
    }

    fn auth(&self) -> Option<&SessionAuth> {
        self.auth.as_ref()
    }
}

#[cfg(feature = "reality")]
#[derive(Clone)]
pub(crate) struct VlessInboundMuxWriter {
    down_tx: mpsc::Sender<BufferedMuxResponse<VlessInboundMuxDownlink>>,
    backlog: MuxResponseBacklog,
}

#[cfg(feature = "reality")]
#[derive(Default)]
struct VlessInboundMuxStreams {
    streams: alloc::collections::BTreeMap<u16, VlessInboundMuxStreamState>,
}

#[cfg(feature = "reality")]
struct VlessInboundMuxStreamState {
    network: MuxNetwork,
    target: Address,
    port: u16,
    upload: mpsc::UnboundedSender<Vec<u8>>,
    termination_probe: zero_core::InboundMuxUdpTerminationProbe,
}

#[cfg(feature = "reality")]
struct VlessInboundMuxDownlink {
    session_id: u16,
    kind: VlessInboundMuxDownlinkKind,
}

#[cfg(feature = "reality")]
enum VlessInboundMuxDownlinkKind {
    Data(Vec<u8>),
    Udp {
        target: Address,
        port: u16,
        payload: Vec<u8>,
    },
    End,
}

#[cfg(feature = "reality")]
pub struct VlessInboundMuxServer {
    mux: VlessInboundMuxSession,
    streams: VlessInboundMuxStreams,
    writer: VlessInboundMuxWriter,
    down_rx: mpsc::Receiver<BufferedMuxResponse<VlessInboundMuxDownlink>>,
    auth: Option<SessionAuth>,
}

#[cfg(feature = "reality")]
impl VlessInboundMuxServer {
    fn new(mux: VlessInboundMuxSession, backlog_policy: MuxResponseBacklogPolicy) -> Self {
        let (writer, down_rx) = VlessInboundMuxWriter::channel(backlog_policy);
        Self {
            mux,
            streams: VlessInboundMuxStreams::new(),
            writer,
            down_rx,
            auth: None,
        }
    }

    pub(crate) fn from_master_uuid_with_auth(
        master_uuid: [u8; 16],
        auth: Option<SessionAuth>,
        backlog_policy: MuxResponseBacklogPolicy,
    ) -> Self {
        Self::new(
            VlessInboundMuxSession::with_encryption(&master_uuid),
            backlog_policy,
        )
        .with_auth(auth)
    }

    fn with_auth(mut self, auth: Option<SessionAuth>) -> Self {
        self.auth = auth;
        self
    }

    fn writer(&self) -> VlessInboundMuxWriter {
        self.writer.clone()
    }

    async fn next_opened_route_with_auth<S>(
        &mut self,
        stream: &mut S,
        auth: Option<&SessionAuth>,
    ) -> Result<Option<VlessInboundMuxOpenedRoute>, Error>
    where
        S: AsyncSocket,
    {
        loop {
            tokio::select! {
                action = self.mux.read_inbound_action(stream) => {
                    if let Some(opened) = self
                        .streams
                        .apply_inbound_action(&mut self.mux, stream, action?)
                        .await?
                    {
                        let writer = self.writer();
                        return Ok(Some(opened.into_route_with_auth(auth, writer)));
                    }
                }
                downlink = self.down_rx.recv() => {
                    let Some(downlink) = downlink else {
                        continue;
                    };
                    let _ = self
                        .streams
                        .send_inbound_downlink(&mut self.mux, stream, downlink.into_inner())
                        .await?;
                }
            }
        }
    }

    async fn next_opened_route<S>(
        &mut self,
        stream: &mut S,
    ) -> Result<Option<VlessInboundMuxOpenedRoute>, Error>
    where
        S: AsyncSocket,
    {
        let auth = self.auth.clone();
        self.next_opened_route_with_auth(stream, auth.as_ref())
            .await
    }
}

#[cfg(feature = "reality")]
#[async_trait::async_trait]
impl<S> zero_core::InboundMuxServer<S> for VlessInboundMuxServer
where
    S: AsyncSocket,
{
    type TcpRelay = VlessInboundMuxTcpRelay;
    type UdpRelay = VlessInboundMuxUdpRelay;

    fn auth(&self) -> Option<&SessionAuth> {
        self.auth.as_ref()
    }

    async fn dispatch_next_opened_route<E, FTcp, FUdp>(
        &mut self,
        stream: &mut S,
        on_tcp_opened: FTcp,
        on_udp_opened: FUdp,
    ) -> Result<bool, E>
    where
        E: From<Error>,
        FTcp: FnOnce(Session, Self::TcpRelay) -> Result<(), E> + Send,
        FUdp: FnOnce(Self::UdpRelay) -> Result<(), E> + Send,
    {
        let Some(route) = self.next_opened_route(stream).await? else {
            return Ok(false);
        };

        match route.state {
            VlessInboundMuxOpenedRouteState::Tcp { session, relay } => {
                on_tcp_opened(*session, relay)?;
            }
            VlessInboundMuxOpenedRouteState::Udp { relay } => {
                on_udp_opened(relay)?;
            }
        }

        Ok(true)
    }
}

#[cfg(feature = "reality")]
impl VlessInboundMuxStreams {
    fn new() -> Self {
        Self::default()
    }

    fn open_stream(
        &mut self,
        session_id: u16,
        session: &Session,
    ) -> (
        mpsc::UnboundedReceiver<Vec<u8>>,
        zero_core::InboundMuxUdpTerminationProbe,
    ) {
        let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let termination_probe = zero_core::InboundMuxUdpTerminationProbe::transport_attached();
        let network = match session.network {
            Network::Tcp => MuxNetwork::Tcp,
            Network::Udp => MuxNetwork::Udp,
        };
        self.streams.insert(
            session_id,
            VlessInboundMuxStreamState {
                network,
                target: session.target.clone(),
                port: session.port,
                upload: tx,
                termination_probe: termination_probe.clone(),
            },
        );
        (rx, termination_probe)
    }

    fn push_stream_data(
        &mut self,
        session_id: u16,
        target: Option<MuxTarget>,
        payload: Vec<u8>,
    ) -> Result<bool, Error> {
        let Some(stream) = self.streams.get_mut(&session_id) else {
            return Ok(false);
        };
        let payload = match stream.network {
            MuxNetwork::Tcp => payload,
            MuxNetwork::Udp => {
                if let Some(target) = target {
                    if target.network_kind()? != MuxNetwork::Udp {
                        return Err(Error::Protocol("MUX UDP stream received a non-UDP target"));
                    }
                    stream.target = target.address;
                    stream.port = target.port;
                }
                crate::udp::encode_udp_flow_packet(&stream.target, stream.port, &payload)?
            }
        };
        Ok(stream.upload.send(payload).is_ok())
    }

    fn close_inbound_stream(&mut self, session_id: u16, explicit_end: bool) -> bool {
        let Some(stream) = self.streams.remove(&session_id) else {
            return false;
        };
        if explicit_end {
            stream.termination_probe.mark_explicit_end();
        }
        true
    }

    fn contains_stream(&self, session_id: u16) -> bool {
        self.streams.contains_key(&session_id)
    }

    async fn apply_inbound_action<S>(
        &mut self,
        mux: &mut VlessInboundMuxSession,
        stream: &mut S,
        action: VlessInboundMuxAction,
    ) -> Result<Option<VlessInboundMuxOpenedStream>, Error>
    where
        S: AsyncSocket,
    {
        match action {
            VlessInboundMuxAction::KeepAlive => Ok(None),
            VlessInboundMuxAction::OpenStream {
                session_id,
                session,
                global_id,
                initial_payload,
            } => {
                if self.contains_stream(session_id) {
                    mux.reject_inbound_stream(stream, session_id).await?;
                    return Ok(None);
                }
                let (up_rx, termination_probe) = self.open_stream(session_id, &session);
                if !initial_payload.is_empty()
                    && !self.push_stream_data(session_id, None, initial_payload)?
                {
                    self.close_inbound_stream(session_id, true);
                    mux.end_inbound_stream(stream, session_id).await?;
                    return Ok(None);
                }
                Ok(Some(VlessInboundMuxOpenedStream::new(
                    session_id,
                    session,
                    global_id,
                    termination_probe,
                    up_rx,
                )))
            }
            VlessInboundMuxAction::Data {
                session_id,
                target,
                payload,
            } => {
                if !self.push_stream_data(session_id, target, payload)? {
                    mux.end_inbound_stream(stream, session_id).await?;
                }
                Ok(None)
            }
            VlessInboundMuxAction::End { session_id } => {
                self.close_inbound_stream(session_id, true);
                Ok(None)
            }
            VlessInboundMuxAction::Unknown { session_id } => {
                mux.reject_inbound_stream(stream, session_id).await?;
                self.close_inbound_stream(session_id, true);
                Ok(None)
            }
        }
    }

    async fn send_inbound_downlink<S>(
        &mut self,
        mux: &mut VlessInboundMuxSession,
        stream: &mut S,
        downlink: VlessInboundMuxDownlink,
    ) -> Result<bool, Error>
    where
        S: AsyncSocket,
    {
        let sid = downlink.session_id();
        if !self.contains_stream(sid) {
            return Ok(false);
        }

        let should_close = downlink.is_end();
        let (sid, kind) = downlink.into_parts();
        match kind {
            VlessInboundMuxDownlinkKind::Data(payload) => {
                mux.send_inbound_stream_data(stream, sid, &payload).await?;
            }
            VlessInboundMuxDownlinkKind::Udp {
                target,
                port,
                payload,
            } => {
                mux.send_inbound_udp_payload(stream, sid, &target, port, &payload)
                    .await?;
            }
            VlessInboundMuxDownlinkKind::End => {
                mux.end_inbound_stream(stream, sid).await?;
            }
        }
        if should_close {
            self.close_inbound_stream(sid, true);
        }
        Ok(true)
    }
}

#[cfg(feature = "reality")]
impl VlessInboundMuxDownlink {
    fn data(session_id: u16, payload: Vec<u8>) -> Self {
        Self {
            session_id,
            kind: VlessInboundMuxDownlinkKind::Data(payload),
        }
    }

    fn udp(session_id: u16, target: Address, port: u16, payload: Vec<u8>) -> Self {
        Self {
            session_id,
            kind: VlessInboundMuxDownlinkKind::Udp {
                target,
                port,
                payload,
            },
        }
    }

    fn end(session_id: u16) -> Self {
        Self {
            session_id,
            kind: VlessInboundMuxDownlinkKind::End,
        }
    }

    fn session_id(&self) -> u16 {
        self.session_id
    }

    fn is_end(&self) -> bool {
        matches!(&self.kind, VlessInboundMuxDownlinkKind::End)
    }

    fn into_parts(self) -> (u16, VlessInboundMuxDownlinkKind) {
        (self.session_id, self.kind)
    }
}

#[cfg(feature = "reality")]
impl VlessInboundMuxWriter {
    fn new(
        down_tx: mpsc::Sender<BufferedMuxResponse<VlessInboundMuxDownlink>>,
        backlog: MuxResponseBacklog,
    ) -> Self {
        Self { down_tx, backlog }
    }

    fn channel(
        policy: MuxResponseBacklogPolicy,
    ) -> (
        Self,
        mpsc::Receiver<BufferedMuxResponse<VlessInboundMuxDownlink>>,
    ) {
        let (down_tx, down_rx) = mpsc::channel(policy.frames());
        let backlog = MuxResponseBacklog::from_policy(policy);
        (Self::new(down_tx, backlog), down_rx)
    }

    fn try_send(&self, bytes: usize, downlink: VlessInboundMuxDownlink) -> Result<(), Error> {
        let response = self
            .backlog
            .try_buffer(bytes, downlink)
            .map_err(|_| Error::Io("VLESS MUX response backlog byte limit exceeded"))?;
        self.down_tx
            .try_send(response)
            .map_err(|_| Error::Io("VLESS MUX response backlog frame limit exceeded"))
    }

    pub(crate) fn data(&self, session_id: u16, payload: Vec<u8>) -> Result<usize, Error> {
        let len = payload.len();
        self.try_send(len, VlessInboundMuxDownlink::data(session_id, payload))?;
        Ok(len)
    }

    pub(crate) fn end(&self, session_id: u16) -> Result<usize, Error> {
        self.try_send(0, VlessInboundMuxDownlink::end(session_id))?;
        Ok(0)
    }

    pub(crate) fn end_inbound_stream(&self, session_id: u16) -> Result<usize, Error> {
        self.end(session_id)
    }

    pub(crate) fn write_inbound_stream_payload(
        &self,
        session_id: u16,
        payload: Vec<u8>,
    ) -> Result<usize, Error> {
        if payload.is_empty() {
            self.end_inbound_stream(session_id)
        } else {
            self.data(session_id, payload)
        }
    }

    pub(crate) fn udp(
        &self,
        session_id: u16,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error> {
        let len = payload.len();
        self.try_send(
            len,
            VlessInboundMuxDownlink::udp(session_id, target.clone(), port, payload.to_vec()),
        )?;
        Ok(len)
    }
}

// ── frame encode / decode ──

#[cfg(feature = "reality")]
async fn relay_inbound_mux_stream<S>(
    session_id: u16,
    mut up_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    writer: VlessInboundMuxWriter,
    mut upstream: S,
) where
    S: AsyncSocket + 'static,
    S::Error: Send,
{
    let mut upload_open = true;
    let mut buf = [0_u8; MUX_MAX_PAYLOAD];

    loop {
        tokio::select! {
            inbound = up_rx.recv(), if upload_open => {
                match inbound {
                    Some(data) => {
                        if upstream.write_all(&data).await.is_err() {
                            break;
                        }
                    }
                    None => {
                        upload_open = false;
                        let _ = upstream.shutdown().await;
                    }
                }
            }
            read = upstream.read(&mut buf) => {
                match read {
                    Ok(0) => break,
                    Ok(n) => {
                        if writer
                            .write_inbound_stream_payload(session_id, buf[..n].to_vec())
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }

    let _ = writer.write_inbound_stream_payload(session_id, Vec::new());
}

// ── Mux.Cool frame encoding ──

/// Build a metadata-only new-stream request using a client-owned session ID.
pub(crate) fn encode_new_stream(
    session_id: u16,
    network: u8,
    port: u16,
    address: &Address,
) -> Result<Vec<u8>, Error> {
    codec::encode_new_stream(session_id, network, port, address)
}

// ── Data / End / KeepAlive frame helpers ──

/// Build a TCP data frame (STATUS_KEEP | OPTION_DATA).
pub(crate) fn encode_data_frame(session_id: u16, data: &[u8]) -> Result<Vec<u8>, Error> {
    codec::encode_data_frame(session_id, data)
}

pub(crate) fn encode_new_udp_data_frame(
    session_id: u16,
    target: &Address,
    port: u16,
    global_id: [u8; 8],
    data: &[u8],
) -> Result<Vec<u8>, Error> {
    codec::encode_new_udp_data_frame(session_id, target, port, global_id, data)
}

pub(crate) fn encode_udp_data_frame(
    session_id: u16,
    target: &Address,
    port: u16,
    data: &[u8],
) -> Result<Vec<u8>, Error> {
    codec::encode_keep_udp_data_frame(session_id, target, port, data)
}

/// Build an END frame (terminate the session).
pub(crate) fn encode_end_frame(session_id: u16) -> Result<Vec<u8>, Error> {
    codec::encode_end_frame(session_id)
}

#[cfg(feature = "reality")]
pub(crate) async fn read_mux_frame_tokio<R>(reader: &mut R) -> Result<MuxFrame, Error>
where
    R: tokio::io::AsyncRead + Unpin,
{
    codec::read_frame_tokio(reader).await
}

// ── Address parsing (internal helper) ──

fn parse_address_from_bytes_with_len(atyp: u8, data: &[u8]) -> Result<(Address, usize), Error> {
    match atyp {
        ATYP_IPV4 => {
            if data.len() < 4 {
                return Err(Error::Protocol("MUX: truncated IPv4 address"));
            }
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(&data[..4]);
            Ok((Address::Ipv4(bytes), 4))
        }
        ATYP_IPV6 => {
            if data.len() < 16 {
                return Err(Error::Protocol("MUX: truncated IPv6 address"));
            }
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&data[..16]);
            Ok((Address::Ipv6(bytes), 16))
        }
        ATYP_DOMAIN => {
            if data.is_empty() {
                return Err(Error::Protocol("MUX: truncated domain address"));
            }
            let len = data[0] as usize;
            if len == 0 || data.len() < 1 + len {
                return Err(Error::Protocol("MUX: truncated domain address"));
            }
            let domain = alloc::string::String::from_utf8(data[1..1 + len].to_vec())
                .map_err(|_| Error::Protocol("MUX domain not valid UTF-8"))?;
            Ok((Address::Domain(domain), 1 + len))
        }
        _ => Err(Error::Unsupported("MUX address type not supported")),
    }
}

// ── mux client ─────────────────────────────────────────

// Minimal MUX client — manages stream allocation and frame I/O.
// ── mux server ─────────────────────────────────────────

/// MUX server-side handler — reads frames and dispatches.
struct MuxServer {}

struct VlessInboundMuxSession {
    server: MuxServer,
}

impl Default for VlessInboundMuxSession {
    fn default() -> Self {
        Self::new()
    }
}

impl VlessInboundMuxSession {
    fn new() -> Self {
        Self {
            server: MuxServer::new(),
        }
    }

    #[cfg(feature = "reality")]
    fn with_encryption(_master_uuid: &[u8; 16]) -> Self {
        Self {
            server: MuxServer {},
        }
    }

    async fn next_event<S>(&mut self, stream: &mut S) -> Result<MuxServerEvent, Error>
    where
        S: AsyncSocket,
    {
        self.server.recv_event(stream).await
    }

    async fn next_action<S>(&mut self, stream: &mut S) -> Result<VlessInboundMuxAction, Error>
    where
        S: AsyncSocket,
    {
        self.next_event(stream).await.map(Into::into)
    }

    async fn read_inbound_action<S>(
        &mut self,
        stream: &mut S,
    ) -> Result<VlessInboundMuxAction, Error>
    where
        S: AsyncSocket,
    {
        self.next_action(stream).await
    }

    async fn reject_stream<S>(&mut self, stream: &mut S, sid: u16) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.server.write_end(stream, sid).await
    }

    async fn reject_inbound_stream<S>(&mut self, stream: &mut S, sid: u16) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.reject_stream(stream, sid).await
    }

    async fn send_data<S>(&mut self, stream: &mut S, sid: u16, payload: &[u8]) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.server.write_data(stream, sid, payload).await
    }

    async fn send_inbound_stream_data<S>(
        &mut self,
        stream: &mut S,
        sid: u16,
        payload: &[u8],
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.send_data(stream, sid, payload).await
    }

    async fn send_inbound_udp_payload<S>(
        &mut self,
        stream: &mut S,
        sid: u16,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.server
            .write_udp_data(stream, sid, target, port, payload)
            .await
    }

    async fn end_stream<S>(&mut self, stream: &mut S, sid: u16) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.server.write_end(stream, sid).await
    }

    async fn end_inbound_stream<S>(&mut self, stream: &mut S, sid: u16) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.end_stream(stream, sid).await
    }
}

impl From<MuxServerEvent> for VlessInboundMuxAction {
    fn from(event: MuxServerEvent) -> Self {
        match event {
            MuxServerEvent::KeepAlive => Self::KeepAlive,
            MuxServerEvent::NewStream {
                session_id,
                target,
                global_id,
                initial_payload,
            } => match target.into_session() {
                Ok(session) => Self::OpenStream {
                    session_id,
                    session: Box::new(session),
                    global_id,
                    initial_payload,
                },
                Err(_) => Self::Unknown { session_id },
            },
            MuxServerEvent::Data {
                session_id,
                target,
                payload,
            } => Self::Data {
                session_id,
                target,
                payload,
            },
            MuxServerEvent::End { session_id } => Self::End { session_id },
            MuxServerEvent::Unknown { session_id } => Self::Unknown { session_id },
        }
    }
}

impl Default for MuxServer {
    fn default() -> Self {
        Self::new()
    }
}

impl MuxServer {
    fn new() -> Self {
        Self {}
    }

    async fn recv_event<S>(&mut self, stream: &mut S) -> Result<MuxServerEvent, Error>
    where
        S: AsyncSocket,
    {
        let frame = self.recv(stream).await?;
        match frame.status {
            STATUS_KEEP_ALIVE => Ok(MuxServerEvent::KeepAlive),
            STATUS_NEW => {
                let target = frame
                    .target
                    .ok_or(Error::Protocol("MUX new stream target is missing"))?;
                Ok(MuxServerEvent::NewStream {
                    session_id: frame.session_id,
                    target,
                    global_id: frame.global_id,
                    initial_payload: frame.payload,
                })
            }
            STATUS_KEEP if frame.options & OPTION_DATA == 0 => Ok(MuxServerEvent::KeepAlive),
            STATUS_KEEP => Ok(MuxServerEvent::Data {
                session_id: frame.session_id,
                target: frame.target,
                payload: frame.payload,
            }),
            STATUS_END => Ok(MuxServerEvent::End {
                session_id: frame.session_id,
            }),
            _status => Ok(MuxServerEvent::Unknown {
                session_id: frame.session_id,
            }),
        }
    }

    /// Read the next standard Mux.Cool frame.
    async fn recv<S>(&mut self, stream: &mut S) -> Result<MuxFrame, Error>
    where
        S: AsyncSocket,
    {
        codec::read_frame(stream).await
    }

    /// Write data to a stream as a STATUS_KEEP frame.
    async fn write_data<S>(&mut self, stream: &mut S, sid: u16, data: &[u8]) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        let frame = encode_data_frame(sid, data)?;
        stream
            .write_all(&frame)
            .await
            .map_err(|_| Error::Io("failed to write MUX data frame"))
    }

    async fn write_udp_data<S>(
        &mut self,
        stream: &mut S,
        sid: u16,
        target: &Address,
        port: u16,
        data: &[u8],
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        let frame = encode_udp_data_frame(sid, target, port, data)?;
        stream
            .write_all(&frame)
            .await
            .map_err(|_| Error::Io("failed to write MUX UDP data frame"))
    }

    /// Write an END frame for a stream.
    async fn write_end<S>(&mut self, stream: &mut S, sid: u16) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        let frame = encode_end_frame(sid)?;
        stream
            .write_all(&frame)
            .await
            .map_err(|_| Error::Io("failed to write MUX end frame"))
    }
}
