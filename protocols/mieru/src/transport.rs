//! Mieru transport-owned UDP bridge helpers.

mod managed_udp;
mod options;

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
    }

    pub fn new(tag: &str, server: &str, port: u16, username: &str, password: &str) -> Self {
        Self {
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

    pub fn server(&self) -> &str {
        &self.server
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn flow_resume(&self, relay_chain: bool) -> MieruManagedUdpFlowResume {
        MieruManagedUdpFlowConfig::new(&self.server, self.port, &self.username, &self.password)
            .flow_resume(relay_chain)
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
    ) -> Result<TcpRelayStream, RuntimeError>
    where
        OpenSocket: Clone + Fn(&str, u16) -> OpenSocketFut + Send + Sync,
        OpenSocketFut: Future<Output = Result<TokioSocket, E>> + Send,
        E: Into<RuntimeError>,
    {
        let socket = open_socket(&self.server, self.port)
            .await
            .map_err(Into::into)?;
        establish_mieru_tcp_tunnel(
            TcpRelayStream::new(socket),
            session,
            &self.username,
            &self.password,
        )
        .await
    }

    pub async fn open_tcp_relay_hop(
        &self,
        stream: TcpRelayStream,
        session: &Session,
    ) -> Result<TcpRelayStream, RuntimeError> {
        establish_mieru_tcp_tunnel(stream, session, &self.username, &self.password).await
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
