//! Mieru transport-owned UDP bridge helpers.

mod managed_udp;
mod options;
mod pool;

use core::future::Future;

use tokio::io::{AsyncRead, AsyncWrite};
use zero_core::{InboundClientResponse, Session};
use zero_platform_tokio::{TcpRelayStream, TokioSocket};
use zero_traits::AsyncSocket;
use zero_transport::RuntimeError;

pub use managed_udp::{
    MieruManagedUdpConnectorFlow, MieruManagedUdpFlowConfig, MieruManagedUdpFlowResume,
};
pub use options::{MieruInboundUserRef, MieruOutboundOptionsRef};

#[derive(Debug, Clone)]
pub struct MieruInboundListenerRequest {
    protocol: crate::inbound::MieruInboundProfile,
}

#[derive(Debug, Default, Clone)]
pub struct MieruInboundResponseProtocol {
    protocol: crate::inbound::MieruInbound,
}

#[derive(Debug, Clone)]
pub struct MieruTransportLeaf {
    options: mieru_config::MieruTransportOptions,
    udp: bool,
    pool: std::sync::Arc<crate::client::ClientPool>,
    tag: String,
    server: String,
    port: u16,
    username: String,
    password: String,
}

#[derive(Debug, Clone)]
pub struct MieruManagedUdpFlowPlan {
    tag: String,
    server: String,
    port: u16,
    resume: MieruManagedUdpFlowResume,
}

impl MieruInboundListenerRequest {
    pub const MAX_PACKET_SIZE: usize = 1500;
    // Cover a socket task's cooperative scheduling burst without unbounded queues.
    pub const PACKET_QUEUE_CAPACITY: usize = 256;

    pub async fn accept_packet_peer(
        &self,
        socket: std::sync::Arc<tokio::net::UdpSocket>,
        peer: std::net::SocketAddr,
        packets: tokio::sync::mpsc::Receiver<Vec<u8>>,
    ) -> std::io::Result<crate::inbound::multiplex::MieruInboundMultiplexer> {
        self.protocol
            .accept_packet_peer(socket, peer, packets)
            .await
    }

    pub fn from_options_refs<'a, I>(users: I) -> Self
    where
        I: IntoIterator<Item = MieruInboundUserRef<'a>>,
    {
        Self::new(crate::inbound::inbound_profile_from_config_users(
            users
                .into_iter()
                .map(|user| (user.username, user.password, user.principal_key)),
        ))
    }

    fn new(protocol: crate::inbound::MieruInboundProfile) -> Self {
        Self { protocol }
    }

    pub fn with_options(mut self, options: mieru_config::MieruTransportOptions) -> Self {
        self.protocol = self.protocol.with_options(options);
        self
    }

    pub async fn accept_multiplexer<S>(
        &self,
        stream: S,
    ) -> Result<crate::inbound::multiplex::MieruInboundMultiplexer, zero_core::Error>
    where
        S: AsyncSocket + AsyncRead + AsyncWrite + Unpin + 'static,
    {
        self.protocol.accept_multiplexer(stream).await
    }

    pub fn response_protocol(&self) -> MieruInboundResponseProtocol {
        MieruInboundResponseProtocol::default()
    }

    pub async fn accept_client<S>(
        &self,
        stream: S,
    ) -> Result<
        crate::inbound::MieruInboundAcceptedSession<crate::inbound::MieruInboundStream<S>>,
        zero_core::Error,
    >
    where
        S: AsyncSocket + AsyncRead + AsyncWrite + Unpin,
    {
        self.protocol.accept_client(stream).await
    }
}

impl<S> InboundClientResponse<crate::inbound::MieruInboundStream<S>>
    for MieruInboundResponseProtocol
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    async fn send_ok(
        &self,
        client: &mut crate::inbound::MieruInboundStream<S>,
    ) -> Result<(), zero_core::Error> {
        self.protocol.send_ok(client).await
    }

    async fn send_blocked(
        &self,
        client: &mut crate::inbound::MieruInboundStream<S>,
    ) -> Result<(), zero_core::Error> {
        self.protocol.send_blocked(client).await
    }

    async fn send_upstream_failure(
        &self,
        client: &mut crate::inbound::MieruInboundStream<S>,
    ) -> Result<(), zero_core::Error> {
        self.protocol.send_upstream_failure(client).await
    }
}

impl MieruTransportLeaf {
    pub fn from_options_refs(
        tag: &str,
        server: &str,
        port: u16,
        options: MieruOutboundOptionsRef<'_>,
    ) -> Self {
        Self::new(tag, server, port, options.username, options.password)
            .with_udp(options.udp)
            .with_options(options.options.clone())
    }

    pub fn new(tag: &str, server: &str, port: u16, username: &str, password: &str) -> Self {
        Self {
            options: Default::default(),
            udp: false,
            pool: Default::default(),
            tag: tag.to_owned(),
            server: server.to_owned(),
            port,
            username: username.to_owned(),
            password: password.to_owned(),
        }
    }

    pub fn tag(&self) -> &str {
        &self.tag
    }

    pub fn with_options(mut self, options: mieru_config::MieruTransportOptions) -> Self {
        self.options = options;
        self
    }

    pub fn server(&self) -> &str {
        &self.server
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn uses_udp_carrier(&self) -> bool {
        self.udp
    }

    pub fn flow_resume(&self, relay_chain: bool) -> MieruManagedUdpFlowResume {
        MieruManagedUdpFlowResume::from_leaf(self.clone(), relay_chain)
    }

    pub fn udp_flow_plan(&self, relay_chain: bool) -> MieruManagedUdpFlowPlan {
        MieruManagedUdpFlowPlan::new(
            self.tag.clone(),
            self.server.clone(),
            self.port,
            self.flow_resume(relay_chain),
        )
    }

    pub async fn open_tcp_stream<OpenSocket, OpenSocketFut, E>(
        &self,
        session: &Session,
        open_socket: OpenSocket,
        factory: zero_transport::OutboundDatagramSocketFactory,
    ) -> Result<TcpRelayStream, RuntimeError>
    where
        OpenSocket: Clone + Fn(&str, u16) -> OpenSocketFut + Send + Sync,
        OpenSocketFut: Future<Output = Result<TokioSocket, E>> + Send,
        E: Into<RuntimeError>,
    {
        let mut stream = self.open_pooled(open_socket, factory).await?;
        crate::tunnel::request_tcp_connect(&mut stream, &session.target, session.port)
            .await
            .map_err(|e| RuntimeError::Io(std::io::Error::other(e.to_string())))?;
        Ok(TcpRelayStream::new(stream))
    }

    pub async fn open_tcp_relay_hop(
        &self,
        stream: TcpRelayStream,
        session: &Session,
    ) -> Result<TcpRelayStream, RuntimeError> {
        if self.udp {
            return Err(RuntimeError::Io(std::io::Error::other(
                "mieru UDP underlay requires a datagram carrier; stream relay is unsupported",
            )));
        }
        let connection = crate::client::ClientConnection::tcp_with_options(
            stream,
            &self.username,
            &self.password,
            &self.options,
        )
        .await
        .map_err(RuntimeError::Io)?;
        let mut stream = connection.open().await.map_err(RuntimeError::Io)?;
        crate::tunnel::request_tcp_connect(&mut stream, &session.target, session.port)
            .await
            .map_err(|e| RuntimeError::Io(std::io::Error::other(e.to_string())))?;
        Ok(TcpRelayStream::new(stream))
    }

    pub async fn open_tcp_relay_hop_lazy<OpenCarrier, OpenCarrierFut, E>(
        &self,
        session: &Session,
        generation: u64,
        relay_identity: &str,
        open_carrier: OpenCarrier,
    ) -> Result<TcpRelayStream, RuntimeError>
    where
        OpenCarrier: FnMut() -> OpenCarrierFut,
        OpenCarrierFut: Future<Output = Result<TcpRelayStream, E>> + Send,
        E: Into<RuntimeError>,
    {
        if self.udp {
            return Err(RuntimeError::Io(std::io::Error::other(
                "mieru UDP underlay requires a datagram carrier",
            )));
        }
        let mut stream = self
            .open_pooled_relay_tcp(generation, relay_identity, open_carrier)
            .await?;
        crate::tunnel::request_tcp_connect(&mut stream, &session.target, session.port)
            .await
            .map_err(|e| RuntimeError::Io(std::io::Error::other(e.to_string())))?;
        Ok(TcpRelayStream::new(stream))
    }
}

pub async fn establish_mieru_tcp_tunnel(
    stream: TcpRelayStream,
    session: &Session,
    username: &str,
    password: &str,
) -> Result<TcpRelayStream, RuntimeError> {
    let mieru_stream = crate::tcp_outbound_profile_from_config(username, password)
        .establish_tcp_tunnel(stream, session)
        .await
        .map_err(|error| {
            RuntimeError::Io(std::io::Error::other(format!("mieru tcp tunnel: {error}")))
        })?;
    Ok(TcpRelayStream::new(mieru_stream))
}

impl MieruManagedUdpFlowPlan {
    fn new(tag: String, server: String, port: u16, resume: MieruManagedUdpFlowResume) -> Self {
        Self {
            tag,
            server,
            port,
            resume,
        }
    }

    pub fn tag(&self) -> &str {
        &self.tag
    }

    pub fn server(&self) -> &str {
        &self.server
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn into_parts(self) -> (String, String, u16, MieruManagedUdpFlowResume) {
        (self.tag, self.server, self.port, self.resume)
    }

    pub fn into_resume(self) -> MieruManagedUdpFlowResume {
        self.resume
    }
}
