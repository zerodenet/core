// QUIC transport — quic.rs
//
// UDP-based transport with TLS 1.3 encryption built-in via QUIC.
// Uses quinn (Rust QUIC implementation).

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

use crate::RuntimeError;
use zero_traits::AsyncSocket;

use zero_platform_tokio::ClientStream;

mod inbound_accept;

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
    _insecure: bool,
    alpn_protocols: &[Vec<u8>],
    sockets: &crate::OutboundDatagramSocketFactory,
) -> Result<QuicStream, RuntimeError> {
    use quinn::crypto::rustls::QuicClientConfig;

    let mut tls_config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .map_err(|error| RuntimeError::Io(io::Error::other(format!("quic tls protocol: {error}"))))?
    .dangerous()
    .with_custom_certificate_verifier(SkipServerVerification::new())
    .with_no_client_auth();

    tls_config.alpn_protocols = alpn_protocols.to_vec();

    let quic_cfg = QuicClientConfig::try_from(tls_config)
        .map_err(|e| RuntimeError::Io(io::Error::other(format!("quic cfg: {e}"))))?;

    let mut client_cfg = quinn::ClientConfig::new(Arc::new(quic_cfg));
    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(std::time::Duration::from_secs(30).try_into().unwrap()));
    client_cfg.transport_config(Arc::new(transport));

    let conn = connect_quic_endpoint(server, port, server_name, client_cfg, sockets).await?;

    let (send, recv) = conn
        .open_bi()
        .await
        .map_err(|e| RuntimeError::Io(io::Error::other(format!("quic open stream: {e}"))))?;

    Ok(QuicStream::new(send, recv))
}

async fn connect_quic_endpoint(
    server: &str,
    port: u16,
    server_name: &str,
    client_config: quinn::ClientConfig,
    sockets: &crate::OutboundDatagramSocketFactory,
) -> Result<quinn::Connection, RuntimeError> {
    let server_addrs = resolve_server_addresses(server, port).await?;
    let mut last_error = None;

    for server_addr in server_addrs {
        let bind_addr = wildcard_bind_addr(server_addr);
        let socket = match sockets.bind_std(server_addr) {
            Ok(socket) => socket,
            Err(error) => {
                last_error = Some(format!("bind {bind_addr} for {server_addr}: {error}"));
                continue;
            }
        };
        let mut endpoint = match quinn::Endpoint::new(
            quinn::EndpointConfig::default(),
            None,
            socket,
            Arc::new(quinn::TokioRuntime),
        ) {
            Ok(endpoint) => endpoint,
            Err(error) => {
                last_error = Some(format!("create endpoint for {server_addr}: {error}"));
                continue;
            }
        };
        endpoint.set_default_client_config(client_config.clone());

        let connecting = match endpoint.connect(server_addr, server_name) {
            Ok(connecting) => connecting,
            Err(error) => {
                last_error = Some(format!("connect {server_addr}: {error}"));
                continue;
            }
        };
        match connecting.await {
            Ok(connection) => return Ok(connection),
            Err(error) => {
                last_error = Some(format!("connect {server_addr}: {error}"));
            }
        }
    }

    Err(RuntimeError::Io(io::Error::other(format!(
        "quic connection to {server}:{port} failed: {}",
        last_error.unwrap_or_else(|| "no resolved address was connectable".to_owned())
    ))))
}

async fn resolve_server_addresses(
    server: &str,
    port: u16,
) -> Result<Vec<SocketAddr>, RuntimeError> {
    let mut addresses = Vec::new();
    let resolved = tokio::net::lookup_host((server, port))
        .await
        .map_err(|error| {
            RuntimeError::Io(io::Error::other(format!(
                "quic resolve {server}:{port}: {error}"
            )))
        })?;

    for address in resolved {
        if !addresses.contains(&address) {
            addresses.push(address);
        }
    }

    if addresses.is_empty() {
        return Err(RuntimeError::Io(io::Error::other(format!(
            "quic resolve {server}:{port}: no addresses returned"
        ))));
    }

    Ok(addresses)
}

fn wildcard_bind_addr(server_addr: SocketAddr) -> SocketAddr {
    match server_addr {
        SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        SocketAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
    }
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

        let mut transport = quinn::TransportConfig::default();
        transport.max_idle_timeout(Some(std::time::Duration::from_secs(30).try_into().unwrap()));
        transport.datagram_receive_buffer_size(Some(65536));
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

// ── SkipServerVerification for QUIC client ──

#[derive(Debug)]
struct SkipServerVerification;

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ED25519,
        ]
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

#[cfg(test)]
mod tests {
    use super::wildcard_bind_addr;
    use std::net::SocketAddr;

    #[test]
    fn uses_ipv4_wildcard_for_ipv4_server() {
        let server: SocketAddr = "192.0.2.1:443".parse().unwrap();

        assert_eq!(wildcard_bind_addr(server), "0.0.0.0:0".parse().unwrap());
    }

    #[test]
    fn uses_ipv6_wildcard_for_ipv6_server() {
        let server: SocketAddr = "[2001:db8::1]:443".parse().unwrap();

        assert_eq!(wildcard_bind_addr(server), "[::]:0".parse().unwrap());
    }
}
