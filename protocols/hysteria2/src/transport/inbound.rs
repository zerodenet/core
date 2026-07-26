use std::path::{Path, PathBuf};

use zero_core::InboundClientResponse;
use zero_traits::AsyncSocket;
use zero_transport::RuntimeError;

use super::{
    inbound_quic_alpn_protocols,
    options::{
        Hysteria2InboundBindOptionsRef, Hysteria2InboundOptionsRef, Hysteria2InboundUserRef,
    },
    Hysteria2AuthenticatedInboundProfile, Hysteria2AuthenticatedQuicConnection,
    Hysteria2InboundTcpResponseProtocol, Hysteria2Stream,
};

#[derive(Debug, Clone)]
pub struct Hysteria2InboundBindPlan {
    cert_path: String,
    key_path: String,
    source_dir: Option<PathBuf>,
}

impl Hysteria2InboundBindPlan {
    pub fn from_options_refs(
        source_dir: Option<&Path>,
        options: Hysteria2InboundBindOptionsRef<'_>,
    ) -> Self {
        Self::from_paths(source_dir, options.cert_path, options.key_path)
    }

    fn from_paths(
        source_dir: Option<&Path>,
        cert_path: Option<&str>,
        key_path: Option<&str>,
    ) -> Self {
        Self {
            cert_path: cert_path.unwrap_or("certs/fullchain.pem").to_owned(),
            key_path: key_path.unwrap_or("certs/privkey.pem").to_owned(),
            source_dir: source_dir.map(PathBuf::from),
        }
    }

    pub async fn bind(
        &self,
        listen_addr: &str,
    ) -> Result<zero_transport::quic::QuicInbound, RuntimeError> {
        let alpn_protocols = inbound_quic_alpn_protocols();
        zero_transport::quic::QuicInbound::bind(
            listen_addr,
            &self.cert_path,
            &self.key_path,
            self.source_dir.as_deref(),
            &alpn_protocols,
        )
        .await
    }
}

fn inbound_tcp_acceptor() -> Hysteria2InboundTcpResponseProtocol {
    Hysteria2InboundTcpResponseProtocol {
        protocol: crate::inbound::Hysteria2InboundTcpAcceptor::new(),
    }
}

impl Hysteria2AuthenticatedInboundProfile {
    pub fn from_options_refs<'a, I>(options: Hysteria2InboundOptionsRef<I>) -> Self
    where
        I: IntoIterator<Item = Hysteria2InboundUserRef<'a>>,
    {
        Self::new(crate::inbound::Hysteria2InboundProfile::from_config_users(
            options.users,
        ))
    }

    fn new(protocol: crate::inbound::Hysteria2InboundProfile) -> Self {
        Self { protocol }
    }

    pub fn with_profile(mut self, profile: crate::inbound::Hysteria2InboundProfile) -> Self {
        self.protocol = profile;
        self
    }

    pub fn tcp_response_protocol(&self) -> Hysteria2InboundTcpResponseProtocol {
        inbound_tcp_acceptor()
    }

    pub async fn accept_authenticated_connection(
        &self,
        connection: quinn::Connection,
    ) -> Result<Hysteria2AuthenticatedQuicConnection, RuntimeError> {
        if super::connection::negotiated_alpn(&connection).as_deref() == Some(b"h3") {
            let raw_connection = connection.clone();
            let (auth, http3) =
                super::auth::authenticate_http3_inbound(connection, &self.protocol).await?;
            let protocol = crate::inbound::Hysteria2AcceptedQuicConnection::new(
                std::sync::Arc::new(raw_connection),
                auth,
            );
            Ok(Hysteria2AuthenticatedQuicConnection {
                protocol,
                _http3: Some(http3),
            })
        } else {
            let protocol = self
                .protocol
                .accept_authenticated_quic_session(connection, Hysteria2Stream::new)
                .await
                .map_err(RuntimeError::from)?;
            Ok(Hysteria2AuthenticatedQuicConnection {
                protocol,
                _http3: None,
            })
        }
    }
}

impl<S> InboundClientResponse<S> for Hysteria2InboundTcpResponseProtocol
where
    S: AsyncSocket,
{
    async fn send_ok(&self, client: &mut S) -> Result<(), zero_core::Error> {
        self.protocol.send_ok(client).await
    }

    async fn send_blocked(&self, client: &mut S) -> Result<(), zero_core::Error> {
        self.protocol.send_blocked(client).await
    }

    async fn send_upstream_failure(&self, client: &mut S) -> Result<(), zero_core::Error> {
        self.protocol.send_upstream_failure(client).await
    }
}

impl Hysteria2AuthenticatedQuicConnection {
    pub fn auth(&self) -> &zero_core::SessionAuth {
        self.protocol.auth()
    }

    pub fn close(&self, reason: &str) {
        self.protocol.close(reason);
    }

    pub fn datagram_source(&self) -> std::sync::Arc<quinn::Connection> {
        self.protocol.connection()
    }

    pub fn udp_relay(&self) -> crate::udp::Hysteria2InboundUdpRelay {
        self.protocol.accept_udp_session()
    }

    pub fn response_protocol(&self) -> Hysteria2InboundTcpResponseProtocol {
        inbound_tcp_acceptor()
    }

    pub async fn accept_next_tcp_stream(
        &self,
    ) -> Result<Option<(zero_core::Session, Hysteria2Stream)>, RuntimeError> {
        self.protocol
            .accept_next_tcp_stream(Hysteria2Stream::new)
            .await
            .map_err(RuntimeError::from)
    }
}
