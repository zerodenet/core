// QUIC transport — quic.rs
//
// UDP-based transport with TLS 1.3 encryption built-in via QUIC.
// Uses quinn (Rust QUIC implementation).

use std::io;
use std::path::Path;
use std::sync::Arc;

use crate::RuntimeError;

pub mod bbr;
mod brutal;
mod client;
mod inbound_accept;
pub mod negotiated;
mod options;
mod stream;
pub use stream::QuicStream;
mod rate_limit;
mod receive_window;
pub use client::{
    client_config, client_config_with_ca, client_config_with_options, connect_quic_endpoint,
    connect_quic_endpoint_masked,
};
pub use options::QuicTransportOptions;
pub use quinn::{Connection as QuicConnection, TransportConfig as QuicTransportConfig};

// ── client (outbound) connect ──

pub async fn connect_quic(
    server: &str,
    port: u16,
    server_name: &str,
    insecure: bool,
    alpn_protocols: &[Vec<u8>],
    sockets: &crate::OutboundDatagramSocketFactory,
) -> Result<QuicStream, RuntimeError> {
    let client_cfg = client::client_config(insecure, None, alpn_protocols, None)?;

    connect_quic_with_config(server, port, server_name, client_cfg, sockets).await
}

pub async fn connect_quic_with_config(
    server: &str,
    port: u16,
    server_name: &str,
    client_cfg: quinn::ClientConfig,
    sockets: &crate::OutboundDatagramSocketFactory,
) -> Result<QuicStream, RuntimeError> {
    let conn = connect_quic_endpoint(server, port, server_name, client_cfg, sockets).await?;

    let (send, recv) = conn
        .open_bi()
        .await
        .map_err(|e| RuntimeError::Io(io::Error::other(format!("quic open stream: {e}"))))?;

    Ok(QuicStream::new(send, recv, &conn))
}

// ── server (inbound) accept ──

pub struct QuicInbound {
    endpoint: quinn::Endpoint,
}

impl QuicInbound {
    pub async fn bind(
        listen_addr: &str,
        cert_path: &str,
        key_path: &str,
        base_dir: Option<&Path>,
        alpn_protocols: &[Vec<u8>],
    ) -> Result<Self, RuntimeError> {
        Self::bind_masked(
            listen_addr,
            cert_path,
            key_path,
            base_dir,
            alpn_protocols,
            &[],
        )
        .await
    }
    pub async fn bind_masked(
        listen_addr: &str,
        cert_path: &str,
        key_path: &str,
        base_dir: Option<&Path>,
        alpn_protocols: &[Vec<u8>],
        masks: &[crate::finalmask::udp::Mask],
    ) -> Result<Self, RuntimeError> {
        let mut transport = quinn::TransportConfig::default();
        transport.max_idle_timeout(Some(std::time::Duration::from_secs(30).try_into().unwrap()));
        transport.datagram_receive_buffer_size(Some(65536));
        Self::bind_with_masks(
            listen_addr,
            cert_path,
            key_path,
            base_dir,
            alpn_protocols,
            transport,
            masks,
        )
        .await
    }

    pub async fn bind_with_transport(
        listen_addr: &str,
        cert_path: &str,
        key_path: &str,
        base_dir: Option<&Path>,
        alpn_protocols: &[Vec<u8>],
        transport: quinn::TransportConfig,
    ) -> Result<Self, RuntimeError> {
        Self::bind_with_masks(
            listen_addr,
            cert_path,
            key_path,
            base_dir,
            alpn_protocols,
            transport,
            &[],
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn bind_with_masks(
        listen_addr: &str,
        cert_path: &str,
        key_path: &str,
        base_dir: Option<&Path>,
        alpn_protocols: &[Vec<u8>],
        transport: quinn::TransportConfig,
        masks: &[crate::finalmask::udp::Mask],
    ) -> Result<Self, RuntimeError> {
        let profile = crate::profile::OwnedServerTlsProfile {
            options: Default::default(),
            cert_path: cert_path.into(),
            key_path: key_path.into(),
            alpn: Vec::new(),
            server_fingerprint: None,
        };
        Self::bind_with_tls_profile(
            listen_addr,
            &profile,
            base_dir,
            alpn_protocols,
            transport,
            masks,
        )
        .await
    }
    pub async fn bind_with_tls_profile(
        listen_addr: &str,
        profile: &(impl zero_traits::ServerTlsProfile + ?Sized),
        base_dir: Option<&Path>,
        alpn_protocols: &[Vec<u8>],
        transport: quinn::TransportConfig,
        masks: &[crate::finalmask::udp::Mask],
    ) -> Result<Self, RuntimeError> {
        let crypto =
            match crate::tls::openssl::quic::build_server_config(profile, base_dir, alpn_protocols)
                .map_err(RuntimeError::Io)?
            {
                Some(crypto) => crypto,
                None => {
                    let mut tls_config = crate::tls::build_server_config(profile, base_dir, true)?;
                    tls_config.alpn_protocols = alpn_protocols.to_vec();
                    let quic_cfg = quinn::crypto::rustls::QuicServerConfig::try_from(tls_config)
                        .map_err(|e| {
                            RuntimeError::Io(io::Error::other(format!("quic server cfg: {e}")))
                        })?;
                    Arc::new(quic_cfg)
                }
            };
        let mut server_cfg = quinn::ServerConfig::with_crypto(crypto);

        server_cfg.transport_config(Arc::new(transport));

        let bind_addr = listen_addr
            .parse::<std::net::SocketAddr>()
            .map_err(|e| RuntimeError::Io(io::Error::other(format!("quic bind addr: {e}"))))?;

        let socket = std::net::UdpSocket::bind(bind_addr)
            .map_err(|e| RuntimeError::Io(io::Error::other(format!("quic bind socket: {e}"))))?;
        socket.set_nonblocking(true)?;
        let socket = quinn::Runtime::wrap_udp_socket(&quinn::TokioRuntime, socket)?;
        let socket = crate::finalmask::Socket::wrap(socket, masks, true)?;
        let endpoint = quinn::Endpoint::new_with_abstract_socket(
            quinn::EndpointConfig::default(),
            Some(server_cfg),
            socket,
            Arc::new(quinn::TokioRuntime),
        )
        .map_err(|e| RuntimeError::Io(io::Error::other(format!("quic endpoint: {e}"))))?;

        Ok(Self { endpoint })
    }
}

/// Configure the HTTP/3 carrier's QUIC liveness policy without protocol framing.
pub fn set_http_keepalive(config: &mut quinn::ClientConfig, seconds: i64) {
    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(
        std::time::Duration::from_secs(300).try_into().unwrap(),
    ));
    // Xray v26.3.27 only applies its H3 default when XMUX's HTTP
    // keepalive option is zero. A nonzero value suppresses that default;
    // unlike H2 it is not interpreted as a QUIC heartbeat interval.
    transport.keep_alive_interval((seconds == 0).then_some(std::time::Duration::from_secs(10)));
    config.transport_config(std::sync::Arc::new(transport));
}

/// Read TLS facts from Quinn's negotiated handshake, never from configuration defaults.
pub fn handshake_metadata(connection: &QuicConnection) -> (Option<String>, Option<String>) {
    let Some(data) = connection.handshake_data() else {
        return Default::default();
    };
    match data.downcast::<quinn::crypto::rustls::HandshakeData>() {
        Ok(data) => (
            data.server_name,
            data.protocol
                .map(|bytes| String::from_utf8_lossy(&bytes).into_owned()),
        ),
        Err(data) => data
            .downcast::<crate::tls::openssl::quic::HandshakeData>()
            .map(|data| {
                (
                    data.server_name,
                    data.protocol
                        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned()),
                )
            })
            .unwrap_or_default(),
    }
}
