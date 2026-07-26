#[cfg(feature = "reality")]
use alloc::borrow::ToOwned;
use alloc::{boxed::Box, vec::Vec};
use core::future::Future;

use zero_core::{Error, Session};
#[cfg(feature = "reality")]
use zero_traits::DeferredTcpTunnelProtocol;
#[cfg(feature = "reality")]
use zero_traits::StreamMuxTransportHints;
use zero_traits::{AsyncSocket, TcpTunnelProtocol};

#[cfg(feature = "tokio")]
use std::pin::Pin;
#[cfg(feature = "tokio")]
use std::task::{Context, Poll};
#[cfg(feature = "tokio")]
use tokio::io::ReadBuf;
#[cfg(feature = "tokio")]
use tokio::io::{AsyncRead, AsyncWrite};

#[cfg(feature = "reality")]
use crate::flow::flow_build_request;
use crate::shared::{parse_uuid, read_response_len, write_address, CMD_MUX, VLESS_VERSION};

#[derive(Debug, Default, Clone, Copy)]
pub struct VlessOutbound;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VlessOutboundParts<'a> {
    id: &'a str,
    flow: Option<&'a str>,
}

impl VlessOutbound {
    async fn establish_tcp_tunnel<S>(
        &self,
        stream: &mut S,
        session: &Session,
        id: &[u8; 16],
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.establish_tcp_tunnel_with_traffic(stream, session, id)
            .await
            .map(|_| ())
    }

    #[cfg(feature = "reality")]
    async fn establish_tcp_tunnel_with_flow<S>(
        &self,
        stream: &mut S,
        session: &Session,
        id: &[u8; 16],
        flow: Option<&str>,
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.establish_tcp_tunnel_with_flow_traffic(stream, session, id, flow)
            .await?;
        Ok(())
    }

    async fn establish_tcp_tunnel_with_traffic<S>(
        &self,
        stream: &mut S,
        session: &Session,
        id: &[u8; 16],
    ) -> Result<(u64, u64), Error>
    where
        S: AsyncSocket,
    {
        let request_len = self.send_tcp_request(stream, session, id).await?;
        let response_len = read_response_len(stream).await?;
        Ok((request_len as u64, response_len as u64))
    }

    #[cfg(feature = "reality")]
    async fn establish_tcp_tunnel_with_flow_traffic<S>(
        &self,
        stream: &mut S,
        session: &Session,
        id: &[u8; 16],
        flow: Option<&str>,
    ) -> Result<(u64, u64), Error>
    where
        S: AsyncSocket,
    {
        let request_len = self
            .send_tcp_request_with_flow(stream, session, id, flow)
            .await?;
        let response_len = read_response_len(stream).await?;
        Ok((request_len as u64, response_len as u64))
    }

    async fn send_tcp_request<S>(
        &self,
        stream: &mut S,
        session: &Session,
        id: &[u8; 16],
    ) -> Result<usize, Error>
    where
        S: AsyncSocket,
    {
        if session.port == 0 {
            return Err(Error::Config("target port is required"));
        }

        let request = build_tcp_request(session, id)?;
        stream
            .write_all(&request)
            .await
            .map_err(|_| Error::Io("failed to write VLESS outbound request"))?;
        Ok(request.len())
    }

    #[cfg(feature = "reality")]
    async fn send_tcp_request_with_flow<S>(
        &self,
        stream: &mut S,
        session: &Session,
        id: &[u8; 16],
        flow: Option<&str>,
    ) -> Result<usize, Error>
    where
        S: AsyncSocket,
    {
        if session.port == 0 {
            return Err(Error::Config("target port is required"));
        }

        let request = build_tcp_request_with_flow(session, id, flow)?;
        stream
            .write_all(&request)
            .await
            .map_err(|_| Error::Io("failed to write VLESS outbound request"))?;
        Ok(request.len())
    }
}

pub(crate) async fn establish_outbound_mux_connection<S>(
    stream: &mut S,
    id: &[u8; 16],
) -> Result<(), Error>
where
    S: AsyncSocket,
{
    let request = build_mux_request(id)?;
    stream
        .write_all(&request)
        .await
        .map_err(|_| Error::Io("failed to write VLESS MUX request"))?;
    Ok(())
}

impl<'a> VlessOutboundParts<'a> {
    fn new(id: &'a str, flow: Option<&'a str>) -> Self {
        Self { id, flow }
    }

    fn tcp_connect_request(self) -> Result<VlessTcpConnectRequest, Error> {
        VlessTcpConnectRequest::from_config(self.id, self.flow)
    }

    fn udp_direct_flow_plan(
        self,
        xudp_concurrency: Option<u32>,
        mux_idle_timeout_secs: Option<u64>,
        mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
    ) -> Result<crate::udp::VlessUdpFlowPlan, Error> {
        crate::udp::VlessUdpFlowPlan::direct_from_config(
            self.id,
            self.flow,
            xudp_concurrency,
            mux_idle_timeout_secs,
            mux_response_backlog,
        )
    }

    fn udp_relay_final_hop_plan(self) -> Result<crate::udp::VlessUdpFlowPlan, Error> {
        crate::udp::VlessUdpFlowPlan::relay_final_hop_from_config(self.id)
    }

    fn udp_relay_paired_transport_plan(self) -> Result<crate::udp::VlessUdpFlowPlan, Error> {
        crate::udp::VlessUdpFlowPlan::relay_paired_transport_from_config(self.id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VlessOutboundRequestBundle {
    tcp_connect: VlessTcpConnectRequest,
    udp_direct: crate::udp::VlessUdpFlowPlan,
    udp_relay_final_hop: crate::udp::VlessUdpFlowPlan,
    udp_relay_paired_transport: crate::udp::VlessUdpFlowPlan,
    mux_concurrency: Option<u32>,
    mux_idle_timeout_secs: Option<u64>,
    mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
}

impl VlessOutboundRequestBundle {
    fn from_config(
        id: &str,
        flow: Option<&str>,
        mux_concurrency: Option<u32>,
        xudp_concurrency: Option<u32>,
        mux_idle_timeout_secs: Option<u64>,
        mux_response_backlog_frames: Option<u32>,
        mux_response_backlog_bytes: Option<u64>,
    ) -> Result<Self, Error> {
        let parts = VlessOutboundParts::new(id, flow);
        let mux_response_backlog = crate::mux::MuxResponseBacklogPolicy::from_config(
            mux_response_backlog_frames,
            mux_response_backlog_bytes,
        )?;
        Ok(Self {
            tcp_connect: parts.tcp_connect_request()?,
            udp_direct: parts.udp_direct_flow_plan(
                xudp_concurrency,
                mux_idle_timeout_secs,
                mux_response_backlog,
            )?,
            udp_relay_final_hop: parts.udp_relay_final_hop_plan()?,
            udp_relay_paired_transport: parts.udp_relay_paired_transport_plan()?,
            mux_concurrency,
            mux_idle_timeout_secs,
            mux_response_backlog,
        })
    }

    fn tcp_connect_request(&self) -> VlessTcpConnectRequest {
        self.tcp_connect
    }

    fn udp_direct_flow_plan(&self) -> crate::udp::VlessUdpFlowPlan {
        self.udp_direct.clone()
    }

    fn udp_relay_final_hop_plan(&self) -> crate::udp::VlessUdpFlowPlan {
        self.udp_relay_final_hop.clone()
    }

    fn udp_relay_paired_transport_plan(&self) -> crate::udp::VlessUdpFlowPlan {
        self.udp_relay_paired_transport.clone()
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

    #[cfg(feature = "reality")]
    fn prepare_with_transport_hints(
        self,
        hints: StreamMuxTransportHints,
    ) -> PreparedVlessOutboundRequestBundle {
        PreparedVlessOutboundRequestBundle::with_transport_profile(
            self,
            crate::mux_pool::OwnedMuxTransportProfile::new(
                hints.tls_server_name().map(str::to_owned),
                hints.reality_public_key().map(str::to_owned),
                hints.reality_server_name().map(str::to_owned),
            ),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedVlessOutboundRequestBundle {
    requests: VlessOutboundRequestBundle,
    #[cfg(feature = "reality")]
    mux_transport_profile: crate::mux_pool::OwnedMuxTransportProfile,
}

impl PreparedVlessOutboundRequestBundle {
    #[cfg(not(feature = "reality"))]
    fn new(requests: VlessOutboundRequestBundle) -> Self {
        Self { requests }
    }

    #[cfg(feature = "reality")]
    fn with_transport_profile(
        requests: VlessOutboundRequestBundle,
        mux_transport_profile: crate::mux_pool::OwnedMuxTransportProfile,
    ) -> Self {
        Self {
            requests,
            mux_transport_profile,
        }
    }

    pub fn from_config(
        id: &str,
        flow: Option<&str>,
        mux_concurrency: Option<u32>,
        xudp_concurrency: Option<u32>,
    ) -> Result<Self, Error> {
        #[cfg(feature = "reality")]
        {
            Self::from_config_with_transport_hints_and_mux_policy(
                id,
                flow,
                mux_concurrency,
                xudp_concurrency,
                None,
                None,
                None,
                StreamMuxTransportHints::default(),
            )
        }

        #[cfg(not(feature = "reality"))]
        {
            VlessOutboundRequestBundle::from_config(
                id,
                flow,
                mux_concurrency,
                xudp_concurrency,
                None,
                None,
                None,
            )
            .map(Self::new)
        }
    }

    #[cfg(feature = "reality")]
    pub fn from_config_with_transport_hints(
        id: &str,
        flow: Option<&str>,
        mux_concurrency: Option<u32>,
        xudp_concurrency: Option<u32>,
        hints: StreamMuxTransportHints,
    ) -> Result<Self, Error> {
        Self::from_config_with_transport_hints_and_mux_policy(
            id,
            flow,
            mux_concurrency,
            xudp_concurrency,
            None,
            None,
            None,
            hints,
        )
    }

    #[cfg(feature = "reality")]
    pub fn from_config_with_transport_hints_and_idle_timeout(
        id: &str,
        flow: Option<&str>,
        mux_concurrency: Option<u32>,
        xudp_concurrency: Option<u32>,
        mux_idle_timeout_secs: Option<u64>,
        hints: StreamMuxTransportHints,
    ) -> Result<Self, Error> {
        Self::from_config_with_transport_hints_and_mux_policy(
            id,
            flow,
            mux_concurrency,
            xudp_concurrency,
            mux_idle_timeout_secs,
            None,
            None,
            hints,
        )
    }

    #[cfg(feature = "reality")]
    #[allow(clippy::too_many_arguments)]
    pub fn from_config_with_transport_hints_and_mux_policy(
        id: &str,
        flow: Option<&str>,
        mux_concurrency: Option<u32>,
        xudp_concurrency: Option<u32>,
        mux_idle_timeout_secs: Option<u64>,
        mux_response_backlog_frames: Option<u32>,
        mux_response_backlog_bytes: Option<u64>,
        hints: StreamMuxTransportHints,
    ) -> Result<Self, Error> {
        VlessOutboundRequestBundle::from_config(
            id,
            flow,
            mux_concurrency,
            xudp_concurrency,
            mux_idle_timeout_secs,
            mux_response_backlog_frames,
            mux_response_backlog_bytes,
        )
        .map(|requests| requests.prepare_with_transport_hints(hints))
    }

    pub async fn establish_tcp_outbound_tunnel<S>(
        &self,
        stream: &mut S,
        session: &Session,
        deferred_response: bool,
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.requests
            .tcp_connect_request()
            .establish_tcp_outbound_tunnel(stream, session, deferred_response)
            .await
    }

    #[cfg(feature = "tokio")]
    pub async fn establish_tcp_outbound_stream<S>(
        &self,
        stream: S,
        session: &Session,
        deferred_response: bool,
    ) -> Result<
        (
            impl AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
            u64,
            u64,
        ),
        Error,
    >
    where
        S: AsyncSocket + AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
    {
        self.requests
            .tcp_connect_request()
            .establish_tcp_outbound_stream(stream, session, deferred_response)
            .await
    }

    #[cfg(feature = "reality")]
    pub async fn open_tcp_stream_with_transport_or_mux<S, OpenStream, OpenStreamFut, E>(
        &self,
        session: &Session,
        server: &str,
        port: u16,
        deferred_response: bool,
        mux_pool: &crate::mux_pool::MuxConnectionPool,
        open_stream: OpenStream,
    ) -> Result<VlessTcpStreamOpen, E>
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
                self.mux_transport_profile.as_borrowed(),
                deferred_response,
                self.requests.mux_concurrency(),
                self.requests.mux_idle_timeout_secs(),
                self.requests.mux_response_backlog(),
                mux_pool,
                open_stream,
            )
            .await
    }

    pub async fn open_tcp_relay_hop_with_transport<S, OpenTransport, OpenTransportFut, E>(
        &self,
        session: &Session,
        quic_requested: bool,
        open_transport: OpenTransport,
    ) -> Result<Box<dyn VlessTcpStreamIo>, E>
    where
        S: AsyncSocket + AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
        OpenTransport: FnOnce() -> OpenTransportFut,
        OpenTransportFut: Future<Output = Result<S, E>>,
        E: From<Error>,
    {
        self.requests
            .tcp_connect_request()
            .open_tcp_relay_hop_with_transport(session, quic_requested, open_transport)
            .await
    }

    pub fn udp_direct_flow_plan(&self) -> crate::udp::PreparedVlessUdpFlowPlan {
        #[cfg(feature = "reality")]
        {
            crate::udp::PreparedVlessUdpFlowPlan::with_transport_profile(
                self.requests.udp_direct_flow_plan(),
                self.mux_transport_profile.clone(),
            )
        }

        #[cfg(not(feature = "reality"))]
        {
            crate::udp::PreparedVlessUdpFlowPlan::new(self.requests.udp_direct_flow_plan())
        }
    }

    pub fn udp_relay_final_hop_plan(&self) -> crate::udp::PreparedVlessUdpFlowPlan {
        #[cfg(feature = "reality")]
        {
            crate::udp::PreparedVlessUdpFlowPlan::with_transport_profile(
                self.requests.udp_relay_final_hop_plan(),
                self.mux_transport_profile.clone(),
            )
        }

        #[cfg(not(feature = "reality"))]
        {
            crate::udp::PreparedVlessUdpFlowPlan::new(self.requests.udp_relay_final_hop_plan())
        }
    }

    pub fn udp_relay_paired_transport_plan(&self) -> crate::udp::PreparedVlessUdpFlowPlan {
        #[cfg(feature = "reality")]
        {
            crate::udp::PreparedVlessUdpFlowPlan::with_transport_profile(
                self.requests.udp_relay_paired_transport_plan(),
                self.mux_transport_profile.clone(),
            )
        }

        #[cfg(not(feature = "reality"))]
        {
            crate::udp::PreparedVlessUdpFlowPlan::new(
                self.requests.udp_relay_paired_transport_plan(),
            )
        }
    }
}

/// Target parameters for VLESS TCP tunnel (non-flow path).
#[derive(Debug, Clone, Copy)]
struct VlessTcpTunnelTarget<'a> {
    pub session: &'a Session,
    pub id: &'a [u8; 16],
}

#[cfg(feature = "tokio")]
enum VlessTcpOutboundStream<S> {
    Standard(S),
    Deferred(crate::deferred_response::DeferredVlessResponseStream<S>),
}

#[cfg(feature = "tokio")]
impl<S> AsyncRead for VlessTcpOutboundStream<S>
where
    S: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.as_mut().get_mut() {
            Self::Standard(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Deferred(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

#[cfg(feature = "tokio")]
impl<S> AsyncWrite for VlessTcpOutboundStream<S>
where
    S: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.as_mut().get_mut() {
            Self::Standard(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Deferred(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.as_mut().get_mut() {
            Self::Standard(stream) => Pin::new(stream).poll_flush(cx),
            Self::Deferred(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.as_mut().get_mut() {
            Self::Standard(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Deferred(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

/// Parsed VLESS identity and flow settings built from external config.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VlessTcpConnectConfig {
    id: [u8; 16],
    flow: Option<&'static str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VlessTcpConnectRequest {
    config: VlessTcpConnectConfig,
}

#[cfg(feature = "tokio")]
pub trait VlessTcpStreamIo: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static {}

#[cfg(feature = "tokio")]
impl<T> VlessTcpStreamIo for T where T: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static {}

#[cfg(feature = "tokio")]
pub struct VlessTcpStreamOpen {
    stream: Box<dyn VlessTcpStreamIo>,
    handshake_written_bytes: u64,
    handshake_read_bytes: u64,
}

#[cfg(feature = "tokio")]
impl VlessTcpStreamOpen {
    fn new(
        stream: Box<dyn VlessTcpStreamIo>,
        handshake_written_bytes: u64,
        handshake_read_bytes: u64,
    ) -> Self {
        Self {
            stream,
            handshake_written_bytes,
            handshake_read_bytes,
        }
    }

    pub fn into_parts(self) -> (Box<dyn VlessTcpStreamIo>, u64, u64) {
        (
            self.stream,
            self.handshake_written_bytes,
            self.handshake_read_bytes,
        )
    }
}

impl VlessTcpConnectRequest {
    pub fn from_config(id: &str, flow: Option<&str>) -> Result<Self, Error> {
        Ok(Self {
            config: VlessTcpConnectConfig::from_config(id, flow)?,
        })
    }

    pub(crate) fn config(&self) -> VlessTcpConnectConfig {
        self.config
    }

    pub async fn establish_tcp_outbound_tunnel<S>(
        &self,
        stream: &mut S,
        session: &Session,
        deferred_response: bool,
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.config()
            .establish_tcp_outbound_tunnel(stream, session, deferred_response)
            .await
    }

    #[cfg(feature = "tokio")]
    pub async fn establish_tcp_outbound_stream<S>(
        &self,
        stream: S,
        session: &Session,
        deferred_response: bool,
    ) -> Result<
        (
            impl AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
            u64,
            u64,
        ),
        Error,
    >
    where
        S: AsyncSocket + AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
    {
        self.config()
            .establish_tcp_outbound_stream(stream, session, deferred_response)
            .await
    }

    #[cfg(feature = "reality")]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn open_tcp_stream_with_transport_or_mux<S, OpenStream, OpenStreamFut, E>(
        &self,
        session: &Session,
        server: &str,
        port: u16,
        profile: crate::mux_pool::MuxTransportProfile<'_>,
        deferred_response: bool,
        mux_concurrency: Option<u32>,
        mux_idle_timeout_secs: Option<u64>,
        mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
        mux_pool: &crate::mux_pool::MuxConnectionPool,
        open_stream: OpenStream,
    ) -> Result<VlessTcpStreamOpen, E>
    where
        S: AsyncSocket + AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
        OpenStream: FnOnce() -> OpenStreamFut,
        OpenStreamFut: Future<Output = Result<S, E>>,
        E: From<Error>,
    {
        let config = self.config();
        if let Some(max_concurrency) = mux_concurrency {
            let key = config.tcp_mux_pool_key_from_transport_config(
                server,
                port,
                profile,
                mux_idle_timeout_secs,
                mux_response_backlog,
            );
            let stream = mux_pool
                .open_tcp_stream(
                    key,
                    max_concurrency,
                    session.port,
                    &session.target,
                    open_stream,
                )
                .await?;
            return Ok(VlessTcpStreamOpen::new(Box::new(stream), 0, 0));
        }

        let stream = open_stream().await?;
        let (stream, handshake_written_bytes, handshake_read_bytes) = config
            .establish_tcp_outbound_stream(stream, session, deferred_response)
            .await
            .map_err(E::from)?;
        Ok(VlessTcpStreamOpen::new(
            Box::new(stream),
            handshake_written_bytes,
            handshake_read_bytes,
        ))
    }

    #[cfg(feature = "tokio")]
    pub(crate) async fn open_tcp_relay_hop_with_transport<S, OpenTransport, OpenTransportFut, E>(
        &self,
        session: &Session,
        quic_requested: bool,
        open_transport: OpenTransport,
    ) -> Result<Box<dyn VlessTcpStreamIo>, E>
    where
        S: AsyncSocket + AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
        OpenTransport: FnOnce() -> OpenTransportFut,
        OpenTransportFut: Future<Output = Result<S, E>>,
        E: From<Error>,
    {
        if quic_requested {
            return Err(E::from(Error::Unsupported(
                "VLESS QUIC relay hop over TCP relay chain is not supported",
            )));
        }

        let stream = open_transport().await?;
        let (stream, _, _) = self
            .config()
            .establish_tcp_outbound_stream(stream, session, true)
            .await
            .map_err(E::from)?;
        Ok(Box::new(stream))
    }
}

impl VlessTcpConnectConfig {
    pub fn from_config(id: &str, flow: Option<&str>) -> Result<Self, Error> {
        #[cfg(feature = "reality")]
        let flow = flow.map(crate::flow::parse_flow).transpose()?;
        #[cfg(not(feature = "reality"))]
        let flow = {
            if flow.is_some() {
                return Err(Error::Unsupported(
                    "VLESS flow requires the `reality` feature",
                ));
            }
            None
        };
        Ok(Self {
            id: parse_uuid(id)?,
            flow,
        })
    }

    #[cfg(feature = "reality")]
    fn has_flow(&self) -> bool {
        self.flow.is_some()
    }

    #[cfg(feature = "reality")]
    fn mux_pool_identity(&self) -> crate::mux_pool::MuxIdentity {
        crate::mux_pool::MuxIdentity::from_uuid(self.id)
    }

    #[cfg(feature = "reality")]
    pub(crate) fn tcp_mux_pool_key_from_transport_config(
        &self,
        server: &str,
        port: u16,
        profile: crate::mux_pool::MuxTransportProfile<'_>,
        mux_idle_timeout_secs: Option<u64>,
        mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
    ) -> crate::mux_pool::PoolKey {
        crate::mux_pool::pool_key_from_transport_config(
            server,
            port,
            self.mux_pool_identity(),
            profile,
            mux_idle_timeout_secs,
            mux_response_backlog,
        )
    }

    async fn establish_tcp_outbound_tunnel_with_traffic<S>(
        &self,
        stream: &mut S,
        session: &Session,
        deferred_response: bool,
    ) -> Result<(u64, u64), Error>
    where
        S: AsyncSocket,
    {
        if deferred_response {
            #[cfg(feature = "reality")]
            let request_len = crate::outbound::VlessOutbound
                .send_tcp_request_with_flow(stream, session, &self.id, self.flow)
                .await?;
            #[cfg(not(feature = "reality"))]
            let request_len = crate::outbound::VlessOutbound
                .send_tcp_request(stream, session, &self.id)
                .await?;
            return Ok((request_len as u64, 0));
        }

        #[cfg(feature = "reality")]
        if self.has_flow() {
            return crate::outbound::VlessOutbound
                .establish_tcp_tunnel_with_flow_traffic(stream, session, &self.id, self.flow)
                .await;
        }

        crate::outbound::VlessOutbound
            .establish_tcp_tunnel_with_traffic(stream, session, &self.id)
            .await
    }

    pub(crate) async fn establish_tcp_outbound_tunnel<S>(
        &self,
        stream: &mut S,
        session: &Session,
        deferred_response: bool,
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.establish_tcp_outbound_tunnel_with_traffic(stream, session, deferred_response)
            .await
            .map(|_| ())
    }

    #[cfg(feature = "tokio")]
    pub(crate) async fn establish_tcp_outbound_stream<S>(
        &self,
        mut stream: S,
        session: &Session,
        deferred_response: bool,
    ) -> Result<
        (
            impl AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
            u64,
            u64,
        ),
        Error,
    >
    where
        S: AsyncSocket + AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
    {
        let (handshake_written_bytes, handshake_read_bytes) = self
            .establish_tcp_outbound_tunnel_with_traffic(&mut stream, session, deferred_response)
            .await?;
        Ok((
            self.wrap_tcp_outbound_stream(stream, deferred_response),
            handshake_written_bytes,
            handshake_read_bytes,
        ))
    }

    #[cfg(feature = "tokio")]
    fn wrap_tcp_outbound_stream<S>(
        &self,
        stream: S,
        deferred_response: bool,
    ) -> impl AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static
    where
        S: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
    {
        if deferred_response {
            VlessTcpOutboundStream::Deferred(
                crate::deferred_response::DeferredVlessResponseStream::new(stream),
            )
        } else {
            VlessTcpOutboundStream::Standard(stream)
        }
    }
}

impl<'a> TcpTunnelProtocol<VlessTcpTunnelTarget<'a>> for VlessOutbound {
    type Error = Error;

    async fn establish_tcp_tunnel<S>(
        &self,
        stream: &mut S,
        target: &VlessTcpTunnelTarget<'a>,
    ) -> Result<(), Self::Error>
    where
        S: AsyncSocket,
    {
        self.establish_tcp_tunnel(stream, target.session, target.id)
            .await
    }
}

/// Target parameters for VLESS TCP tunnel with flow (Vision/Reality path).
///
/// The flow parameter controls the XTLS Vision flow negotiation. When `None`,
/// the handshake uses the standard path. This target is only available when
/// the `reality` feature is enabled because flow handling requires the
/// Vision/Reality code path.
#[cfg(feature = "reality")]
#[derive(Debug, Clone, Copy)]
struct VlessFlowTcpTunnelTarget<'a> {
    pub session: &'a Session,
    pub id: &'a [u8; 16],
    pub flow: Option<&'a str>,
}

#[cfg(feature = "reality")]
impl<'a> TcpTunnelProtocol<VlessFlowTcpTunnelTarget<'a>> for VlessOutbound {
    type Error = Error;

    async fn establish_tcp_tunnel<S>(
        &self,
        stream: &mut S,
        target: &VlessFlowTcpTunnelTarget<'a>,
    ) -> Result<(), Self::Error>
    where
        S: AsyncSocket,
    {
        match target.flow {
            Some(f) => {
                self.establish_tcp_tunnel_with_flow(stream, target.session, target.id, Some(f))
                    .await
            }
            None => {
                self.establish_tcp_tunnel(stream, target.session, target.id)
                    .await
            }
        }
    }
}

#[cfg(feature = "reality")]
impl<'a> DeferredTcpTunnelProtocol<VlessFlowTcpTunnelTarget<'a>> for VlessOutbound {
    type Error = Error;

    async fn send_deferred_tcp_tunnel_request<S>(
        &self,
        stream: &mut S,
        target: &VlessFlowTcpTunnelTarget<'a>,
    ) -> Result<(), Self::Error>
    where
        S: AsyncSocket,
    {
        self.send_tcp_request_with_flow(stream, target.session, target.id, target.flow)
            .await
            .map(|_| ())
    }
}

fn build_tcp_request(session: &Session, id: &[u8; 16]) -> Result<Vec<u8>, Error> {
    let mut request = Vec::with_capacity(24);
    request.push(VLESS_VERSION);
    request.extend_from_slice(id);
    request.push(0x00);
    request.push(crate::shared::CMD_TCP);
    request.extend_from_slice(&session.port.to_be_bytes());
    write_address(&mut request, &session.target)?;

    Ok(request)
}

fn build_mux_request(id: &[u8; 16]) -> Result<Vec<u8>, Error> {
    let mut request = Vec::with_capacity(19);
    request.push(VLESS_VERSION);
    request.extend_from_slice(id);
    request.push(0x00);
    request.push(CMD_MUX);

    Ok(request)
}

#[cfg(feature = "reality")]
fn build_tcp_request_with_flow(
    session: &Session,
    id: &[u8; 16],
    flow: Option<&str>,
) -> Result<Vec<u8>, Error> {
    let (fbyte, payload) = flow_build_request(
        id,
        flow,
        crate::shared::CMD_TCP,
        session.port,
        &session.target,
    )?;

    let mut request = Vec::with_capacity(24 + payload.len());
    request.push(VLESS_VERSION);
    request.extend_from_slice(id);
    request.push(fbyte);
    request.extend_from_slice(&payload);

    Ok(request)
}
