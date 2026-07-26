use core::future::Future;
use std::boxed::Box;

use crate::shared::{
    establish_outbound_session, establish_outbound_session_with_request_len, VmessOutboundSession,
    CMD_TCP,
};
use crate::stream::VmessAeadStream;
use crate::{parse_uuid, VmessCipher};
use tokio::io::{AsyncRead, AsyncWrite};
use zero_core::{Error, Session};
use zero_traits::{AsyncSocket, StreamMuxTransportHints};

#[derive(Debug, Clone, Copy)]
pub struct VmessOutbound;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VmessOutboundParts<'a> {
    id: &'a str,
    cipher: &'a str,
    mux_concurrency: Option<u32>,
    mux_idle_timeout_secs: Option<u64>,
}

impl VmessOutbound {
    pub(crate) async fn establish_tcp_session<S: AsyncSocket>(
        &self,
        stream: &mut S,
        session: &Session,
        uuid: &[u8; 16],
        cipher: VmessCipher,
    ) -> Result<VmessOutboundSession, Error> {
        establish_outbound_session(stream, session, uuid, cipher, CMD_TCP).await
    }
}

impl<'a> VmessOutboundParts<'a> {
    fn new(
        id: &'a str,
        cipher: &'a str,
        mux_concurrency: Option<u32>,
        mux_idle_timeout_secs: Option<u64>,
    ) -> Self {
        Self {
            id,
            cipher,
            mux_concurrency,
            mux_idle_timeout_secs,
        }
    }

    fn tcp_connect_request(self) -> Result<VmessTcpConnectRequest, Error> {
        VmessTcpConnectRequest::from_config(self.id, self.cipher)
    }

    fn udp_direct_flow_plan(
        self,
        mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
    ) -> Result<crate::udp::VmessUdpFlowPlan, Error> {
        crate::udp::VmessUdpFlowPlan::direct_from_config(
            self.id,
            self.cipher,
            self.mux_concurrency,
            self.mux_idle_timeout_secs,
            mux_response_backlog,
        )
    }

    fn udp_relay_flow_plan(self) -> Result<crate::udp::VmessUdpFlowPlan, Error> {
        crate::udp::VmessUdpFlowPlan::relay_from_config(self.id, self.cipher)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VmessOutboundRequestBundle {
    tcp_connect: VmessTcpConnectRequest,
    udp_direct: crate::udp::VmessUdpFlowPlan,
    udp_relay: crate::udp::VmessUdpFlowPlan,
    mux_concurrency: Option<u32>,
    mux_idle_timeout_secs: Option<u64>,
    mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
}

impl VmessOutboundRequestBundle {
    fn from_config(
        id: &str,
        cipher: &str,
        mux_concurrency: Option<u32>,
        mux_idle_timeout_secs: Option<u64>,
        mux_response_backlog_frames: Option<u32>,
        mux_response_backlog_bytes: Option<u64>,
    ) -> Result<Self, Error> {
        let parts = VmessOutboundParts::new(id, cipher, mux_concurrency, mux_idle_timeout_secs);
        let mux_response_backlog = crate::mux::MuxResponseBacklogPolicy::from_config(
            mux_response_backlog_frames,
            mux_response_backlog_bytes,
        )?;
        Ok(Self {
            tcp_connect: parts.tcp_connect_request()?,
            udp_direct: parts.udp_direct_flow_plan(mux_response_backlog)?,
            udp_relay: parts.udp_relay_flow_plan()?,
            mux_concurrency,
            mux_idle_timeout_secs,
            mux_response_backlog,
        })
    }

    fn tcp_connect_request(&self) -> VmessTcpConnectRequest {
        self.tcp_connect.clone()
    }

    fn udp_direct_flow_plan(&self) -> crate::udp::VmessUdpFlowPlan {
        self.udp_direct.clone()
    }

    fn udp_relay_flow_plan(&self) -> crate::udp::VmessUdpFlowPlan {
        self.udp_relay.clone()
    }

    fn mux_concurrency(&self) -> Option<u32> {
        self.mux_concurrency
    }

    fn mux_idle_timeout_secs(&self) -> Option<u64> {
        self.mux_idle_timeout_secs
    }

    fn mux_response_backlog(&self) -> crate::mux::MuxResponseBacklogPolicy {
        self.mux_response_backlog
    }

    fn prepare_with_transport_hints(
        self,
        hints: StreamMuxTransportHints,
    ) -> PreparedVmessOutboundRequestBundle {
        PreparedVmessOutboundRequestBundle::new(
            self,
            crate::mux::OwnedVmessMuxTransportProfile::new(
                hints.tls_server_name().map(str::to_owned),
                hints.ws_path().map(str::to_owned),
                hints.grpc_service_names().map(|names| names.to_vec()),
            ),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedVmessOutboundRequestBundle {
    requests: VmessOutboundRequestBundle,
    mux_transport_profile: crate::mux::OwnedVmessMuxTransportProfile,
}

impl PreparedVmessOutboundRequestBundle {
    fn new(
        requests: VmessOutboundRequestBundle,
        mux_transport_profile: crate::mux::OwnedVmessMuxTransportProfile,
    ) -> Self {
        Self {
            requests,
            mux_transport_profile,
        }
    }

    pub fn from_config(
        id: &str,
        cipher: &str,
        mux_concurrency: Option<u32>,
    ) -> Result<Self, Error> {
        Self::from_config_with_transport_hints_and_mux_policy(
            id,
            cipher,
            mux_concurrency,
            None,
            None,
            None,
            StreamMuxTransportHints::default(),
        )
    }

    pub fn from_config_with_transport_hints(
        id: &str,
        cipher: &str,
        mux_concurrency: Option<u32>,
        hints: StreamMuxTransportHints,
    ) -> Result<Self, Error> {
        Self::from_config_with_transport_hints_and_mux_policy(
            id,
            cipher,
            mux_concurrency,
            None,
            None,
            None,
            hints,
        )
    }

    pub fn from_config_with_transport_hints_and_idle_timeout(
        id: &str,
        cipher: &str,
        mux_concurrency: Option<u32>,
        mux_idle_timeout_secs: Option<u64>,
        hints: StreamMuxTransportHints,
    ) -> Result<Self, Error> {
        Self::from_config_with_transport_hints_and_mux_policy(
            id,
            cipher,
            mux_concurrency,
            mux_idle_timeout_secs,
            None,
            None,
            hints,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_config_with_transport_hints_and_mux_policy(
        id: &str,
        cipher: &str,
        mux_concurrency: Option<u32>,
        mux_idle_timeout_secs: Option<u64>,
        mux_response_backlog_frames: Option<u32>,
        mux_response_backlog_bytes: Option<u64>,
        hints: StreamMuxTransportHints,
    ) -> Result<Self, Error> {
        VmessOutboundRequestBundle::from_config(
            id,
            cipher,
            mux_concurrency,
            mux_idle_timeout_secs,
            mux_response_backlog_frames,
            mux_response_backlog_bytes,
        )
        .map(|requests| requests.prepare_with_transport_hints(hints))
    }

    pub async fn open_tcp_stream_with_transport_or_mux<S, OpenStream, OpenStreamFut, E>(
        &self,
        session: &Session,
        server: &str,
        port: u16,
        mux_pool: &crate::mux::VmessMuxConnectionPool,
        open_stream: OpenStream,
    ) -> Result<VmessTcpStreamOpen, E>
    where
        S: AsyncSocket + AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
        OpenStream: FnOnce() -> OpenStreamFut,
        OpenStreamFut: Future<Output = Result<S, E>>,
        E: From<Error>,
    {
        self.requests
            .tcp_connect_request()
            .open_tcp_stream_with_transport_or_mux(
                session,
                server,
                port,
                self.requests.mux_concurrency(),
                self.requests.mux_idle_timeout_secs(),
                self.requests.mux_response_backlog(),
                self.mux_transport_profile.as_borrowed(),
                mux_pool,
                open_stream,
            )
            .await
    }

    pub async fn establish_tcp_outbound_stream<S>(
        &self,
        stream: S,
        session: &Session,
    ) -> Result<
        (
            impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
            u64,
        ),
        Error,
    >
    where
        S: AsyncSocket
            + tokio::io::AsyncRead
            + tokio::io::AsyncWrite
            + Send
            + Sync
            + Unpin
            + 'static,
    {
        self.requests
            .tcp_connect_request()
            .establish_tcp_outbound_stream(stream, session)
            .await
    }

    pub async fn open_tcp_relay_hop_with_transport<S, OpenTransport, OpenTransportFut, E>(
        &self,
        session: &Session,
        open_transport: OpenTransport,
    ) -> Result<Box<dyn VmessTcpStreamIo>, E>
    where
        S: AsyncSocket + AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
        OpenTransport: FnOnce() -> OpenTransportFut,
        OpenTransportFut: Future<Output = Result<S, E>>,
        E: From<Error>,
    {
        self.requests
            .tcp_connect_request()
            .open_tcp_relay_hop_with_transport(session, open_transport)
            .await
    }

    pub fn udp_direct_flow_plan(&self) -> crate::udp::PreparedVmessUdpFlowPlan {
        crate::udp::PreparedVmessUdpFlowPlan::new(
            self.requests.udp_direct_flow_plan(),
            self.mux_transport_profile.clone(),
        )
    }

    pub fn udp_relay_flow_plan(&self) -> crate::udp::PreparedVmessUdpFlowPlan {
        crate::udp::PreparedVmessUdpFlowPlan::new(
            self.requests.udp_relay_flow_plan(),
            self.mux_transport_profile.clone(),
        )
    }
}

/// Parsed VMess identity settings built from external config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VmessTcpConnectConfig {
    uuid: [u8; 16],
    cipher_name: String,
    cipher: VmessCipher,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VmessTcpConnectRequest {
    config: VmessTcpConnectConfig,
}

pub trait VmessTcpStreamIo: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static {}

impl<T> VmessTcpStreamIo for T where T: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static {}

pub struct VmessTcpStreamOpen {
    stream: Box<dyn VmessTcpStreamIo>,
    handshake_written_bytes: u64,
}

impl VmessTcpStreamOpen {
    fn new(stream: Box<dyn VmessTcpStreamIo>, handshake_written_bytes: u64) -> Self {
        Self {
            stream,
            handshake_written_bytes,
        }
    }

    pub fn into_parts(self) -> (Box<dyn VmessTcpStreamIo>, u64) {
        (self.stream, self.handshake_written_bytes)
    }
}

impl VmessTcpConnectRequest {
    pub fn from_config(id: &str, cipher: &str) -> Result<Self, Error> {
        Ok(Self {
            config: VmessTcpConnectConfig::from_config(id, cipher)?,
        })
    }

    pub(crate) fn config(&self) -> &VmessTcpConnectConfig {
        &self.config
    }

    pub async fn establish_tcp_outbound_stream<S>(
        &self,
        stream: S,
        session: &Session,
    ) -> Result<
        (
            impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
            u64,
        ),
        Error,
    >
    where
        S: AsyncSocket
            + tokio::io::AsyncRead
            + tokio::io::AsyncWrite
            + Send
            + Sync
            + Unpin
            + 'static,
    {
        self.config()
            .establish_tcp_outbound_stream(stream, session)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn open_tcp_stream_with_transport_or_mux<S, OpenStream, OpenStreamFut, E>(
        &self,
        session: &Session,
        server: &str,
        port: u16,
        mux_concurrency: Option<u32>,
        mux_idle_timeout_secs: Option<u64>,
        mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
        profile: crate::mux::VmessMuxTransportProfile<'_>,
        mux_pool: &crate::mux::VmessMuxConnectionPool,
        open_stream: OpenStream,
    ) -> Result<VmessTcpStreamOpen, E>
    where
        S: AsyncSocket + AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
        OpenStream: FnOnce() -> OpenStreamFut,
        OpenStreamFut: Future<Output = Result<S, E>>,
        E: From<Error>,
    {
        let config = self.config();
        if let Some(max_concurrency) = mux_concurrency {
            let key = config
                .tcp_mux_pool_key_from_transport_config(
                    server,
                    port,
                    profile,
                    mux_idle_timeout_secs,
                    mux_response_backlog,
                )
                .map_err(E::from)?;
            let stream = mux_pool
                .open_tcp_stream(
                    key,
                    max_concurrency,
                    session.target.clone(),
                    session.port,
                    open_stream,
                )
                .await?;
            return Ok(VmessTcpStreamOpen::new(Box::new(stream), 0));
        }

        let stream = open_stream().await?;
        let (stream, handshake_written_bytes) = config
            .establish_tcp_outbound_stream(stream, session)
            .await
            .map_err(E::from)?;
        Ok(VmessTcpStreamOpen::new(
            Box::new(stream),
            handshake_written_bytes,
        ))
    }

    pub(crate) async fn open_tcp_relay_hop_with_transport<S, OpenTransport, OpenTransportFut, E>(
        &self,
        session: &Session,
        open_transport: OpenTransport,
    ) -> Result<Box<dyn VmessTcpStreamIo>, E>
    where
        S: AsyncSocket + AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
        OpenTransport: FnOnce() -> OpenTransportFut,
        OpenTransportFut: Future<Output = Result<S, E>>,
        E: From<Error>,
    {
        let stream = open_transport().await?;
        let (stream, _handshake_written_bytes) = self
            .config()
            .establish_tcp_outbound_stream(stream, session)
            .await
            .map_err(E::from)?;
        Ok(Box::new(stream))
    }
}

impl VmessTcpConnectConfig {
    pub fn from_config(id: &str, cipher: &str) -> Result<Self, Error> {
        let uuid = parse_uuid(id)?;
        let cipher =
            VmessCipher::from_name(cipher).ok_or(Error::Protocol("vmess unknown cipher"))?;
        Ok(Self {
            uuid,
            cipher_name: cipher.name().to_owned(),
            cipher,
        })
    }

    fn mux_pool_identity(&self) -> crate::mux::VmessMuxIdentity {
        crate::mux::VmessMuxIdentity::from_parts(self.uuid, self.cipher_name.clone(), self.cipher)
    }

    pub(crate) fn tcp_mux_pool_key_from_transport_config(
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

    async fn establish_tcp_outbound_session_with_request_len<S>(
        &self,
        stream: &mut S,
        session: &Session,
    ) -> Result<(VmessOutboundSession, usize), Error>
    where
        S: AsyncSocket,
    {
        establish_outbound_session_with_request_len(
            stream,
            session,
            &self.uuid,
            self.cipher,
            CMD_TCP,
        )
        .await
    }

    pub(crate) async fn establish_tcp_outbound_stream<S>(
        &self,
        mut stream: S,
        session: &Session,
    ) -> Result<
        (
            impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
            u64,
        ),
        Error,
    >
    where
        S: AsyncSocket
            + tokio::io::AsyncRead
            + tokio::io::AsyncWrite
            + Send
            + Sync
            + Unpin
            + 'static,
    {
        let (vmess_session, request_len) = self
            .establish_tcp_outbound_session_with_request_len(&mut stream, session)
            .await?;
        Ok((
            self.wrap_tcp_outbound_stream(stream, vmess_session)?,
            request_len as u64,
        ))
    }

    fn wrap_tcp_outbound_stream<S>(
        &self,
        stream: S,
        session: VmessOutboundSession,
    ) -> Result<
        impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
        Error,
    >
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
    {
        VmessAeadStream::outbound(stream, session)
    }
}
