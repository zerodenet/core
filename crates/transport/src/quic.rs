// QUIC transport — quic.rs
//
// UDP-based transport with TLS 1.3 encryption built-in via QUIC.
// Uses quinn (Rust QUIC implementation).

use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

use crate::RuntimeError;
use zero_traits::AsyncSocket;

use zero_platform_tokio::ClientStream;

pub mod bbr;
mod client;
mod inbound_accept;
mod options;
mod rate_limit;
mod receive_window;
pub use client::{client_config, connect_quic_endpoint};
pub use options::QuicTransportOptions;

/// Bidirectional QUIC stream wrapping quinn SendStream and RecvStream.
pub struct QuicStream {
    send: quinn::SendStream,
    recv: quinn::RecvStream,
}

impl QuicStream {
    fn new(send: quinn::SendStream, recv: quinn::RecvStream) -> Self {
        Self { send, recv }
    }
}

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

    let conn = connect_quic_endpoint(server, port, server_name, client_cfg, sockets).await?;

    let (send, recv) = conn
        .open_bi()
        .await
        .map_err(|e| RuntimeError::Io(io::Error::other(format!("quic open stream: {e}"))))?;

    Ok(QuicStream::new(send, recv))
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
        let mut transport = quinn::TransportConfig::default();
        transport.max_idle_timeout(Some(std::time::Duration::from_secs(30).try_into().unwrap()));
        transport.datagram_receive_buffer_size(Some(65536));
        Self::bind_with_transport(
            listen_addr,
            cert_path,
            key_path,
            base_dir,
            alpn_protocols,
            transport,
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
        use std::fs::File;
        use std::io::BufReader;

        let cert_path = resolve_path(base_dir, cert_path);
        let key_path = resolve_path(base_dir, key_path);

        let cert_file = File::open(&cert_path).map_err(|e| {
            RuntimeError::Io(io::Error::other(format!(
                "quic cert file `{}`: {e}",
                cert_path.display()
            )))
        })?;
        let mut reader = BufReader::new(cert_file);
        let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
            rustls_pemfile::certs(&mut reader)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| RuntimeError::Io(io::Error::other(format!("quic cert parse: {e}"))))?;

        let key_file = File::open(&key_path).map_err(|e| {
            RuntimeError::Io(io::Error::other(format!(
                "quic key file `{}`: {e}",
                key_path.display()
            )))
        })?;
        let mut reader = BufReader::new(key_file);
        let key = rustls_pemfile::private_key(&mut reader)
            .map_err(|e| RuntimeError::Io(io::Error::other(format!("quic key parse: {e}"))))?
            .ok_or_else(|| {
                RuntimeError::Io(io::Error::other("quic key file contains no private key"))
            })?;

        let mut tls_config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|error| RuntimeError::Io(io::Error::other(format!("quic tls protocol: {error}"))))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| RuntimeError::Io(io::Error::other(format!("quic server tls cfg: {e}"))))?;
        tls_config.alpn_protocols = alpn_protocols.to_vec();

        let quic_cfg = quinn::crypto::rustls::QuicServerConfig::try_from(tls_config)
            .map_err(|e| RuntimeError::Io(io::Error::other(format!("quic server cfg: {e}"))))?;
        let mut server_cfg = quinn::ServerConfig::with_crypto(Arc::new(quic_cfg));

        server_cfg.transport_config(Arc::new(transport));

        let bind_addr = listen_addr
            .parse::<std::net::SocketAddr>()
            .map_err(|e| RuntimeError::Io(io::Error::other(format!("quic bind addr: {e}"))))?;

        let socket = std::net::UdpSocket::bind(bind_addr)
            .map_err(|e| RuntimeError::Io(io::Error::other(format!("quic bind socket: {e}"))))?;
        let endpoint = quinn::Endpoint::new(
            quinn::EndpointConfig::default(),
            Some(server_cfg),
            socket,
            Arc::new(quinn::TokioRuntime),
        )
        .map_err(|e| RuntimeError::Io(io::Error::other(format!("quic endpoint: {e}"))))?;

        Ok(Self { endpoint })
    }
}

// ── AsyncRead / AsyncWrite / AsyncSocket / ClientStream ──

impl AsyncRead for QuicStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.recv).poll_read(cx, buf)
    }
}

impl AsyncWrite for QuicStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.send)
            .poll_write(cx, buf)
            .map_err(io::Error::other)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.send)
            .poll_flush(cx)
            .map_err(io::Error::other)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.send)
            .poll_shutdown(cx)
            .map_err(io::Error::other)
    }
}

impl AsyncSocket for QuicStream {
    type Error = io::Error;

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        AsyncReadExt::read(self, buf).await
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        AsyncWriteExt::write_all(self, buf).await?;
        AsyncWriteExt::flush(self).await
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        AsyncWriteExt::shutdown(self).await
    }
}

impl ClientStream for QuicStream {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "QuicStream does not expose local_addr",
        ))
    }
}

fn resolve_path(base_dir: Option<&Path>, path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        return path;
    }
    base_dir
        .map(|base_dir| base_dir.join(&path))
        .unwrap_or(path)
}
