use std::io;
use std::path::Path;
use std::sync::Arc;

use rustls::{ClientConfig, RootCertStore};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
pub use tokio_rustls::TlsAcceptor;
use tokio_rustls::TlsConnector;
use zero_platform_tokio::TokioSocket;
use zero_traits::{ClientTlsProfile, ServerTlsProfile};

use crate::RuntimeError;
use zero_platform_tokio::TcpRelayStream;

mod certificates;
mod client_hello;
mod inbound_stream;

use certificates::{load_certs, load_private_key, resolve_path};
use client_hello::{parse_extensions, read_exact, skip_exact};
pub use inbound_stream::InboundTlsStream;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct InboundClientHello {
    pub sni: Option<String>,
    pub alpn: Vec<String>,
    pub consumed: Vec<u8>,
}

#[derive(Debug)]
struct InsecureCertVerifier;

impl rustls::client::danger::ServerCertVerifier for InsecureCertVerifier {
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
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

pub fn build_tls_acceptor<T>(tls: &T, base_dir: Option<&Path>) -> Result<TlsAcceptor, RuntimeError>
where
    T: ServerTlsProfile + ?Sized,
{
    let certs = load_certs(&resolve_path(base_dir, tls.cert_path()))?;
    let key = load_private_key(&resolve_path(base_dir, tls.key_path()))?;

    // Look up server fingerprint preset for cipher suite preference control
    let fingerprint = tls.server_fingerprint().and_then(|name| {
            let fp = crate::fingerprint::lookup_fingerprint(name);
            if fp.is_none() {
                tracing::warn!(fingerprint = %name, "unknown tls server fingerprint preset, using defaults");
            }
            fp
        });

    let config_builder = if let Some(ref fp) = fingerprint {
        let provider = Arc::new(crate::fingerprint::build_provider(fp));
        tracing::debug!(
            fingerprint = %tls.server_fingerprint().unwrap_or(""),
            cipher_count = fp.cipher_suites.len(),
            "tls server fingerprint applied"
        );
        rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
    } else {
        rustls::ServerConfig::builder()
    };

    let mut config = config_builder
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;

    if !tls.alpn().is_empty() {
        config.alpn_protocols = tls
            .alpn()
            .iter()
            .map(|proto| proto.as_bytes().to_vec())
            .collect();
    }

    Ok(TlsAcceptor::from(Arc::new(config)))
}

pub async fn accept_tls_inbound(
    stream: TokioSocket,
    acceptor: &TlsAcceptor,
) -> Result<InboundTlsStream<TokioSocket>, RuntimeError> {
    let tls = acceptor
        .accept(stream)
        .await
        .map_err(|error| RuntimeError::Io(io::Error::other(error)))?;
    Ok(InboundTlsStream::new_generic(tls))
}

pub async fn peek_client_hello<R>(reader: &mut R) -> io::Result<Option<InboundClientHello>>
where
    R: AsyncRead + Unpin,
{
    let mut consumed = Vec::with_capacity(512);

    let mut record_hdr = [0u8; 5];
    reader.read_exact(&mut record_hdr).await?;
    if record_hdr[0] != 0x16 {
        return Ok(None);
    }
    consumed.extend_from_slice(&record_hdr);

    let mut handshake_hdr = [0u8; 4];
    reader.read_exact(&mut handshake_hdr).await?;
    if handshake_hdr[0] != 0x01 {
        return Ok(None);
    }
    consumed.extend_from_slice(&handshake_hdr);

    let mut fixed = [0u8; 35];
    read_exact(reader, &mut consumed, &mut fixed).await?;
    let session_id_len = fixed[34] as usize;
    skip_exact(reader, &mut consumed, session_id_len).await?;

    let mut cipher_suites_len = [0u8; 2];
    read_exact(reader, &mut consumed, &mut cipher_suites_len).await?;
    skip_exact(
        reader,
        &mut consumed,
        u16::from_be_bytes(cipher_suites_len) as usize,
    )
    .await?;

    let mut compression_methods_len = [0u8; 1];
    read_exact(reader, &mut consumed, &mut compression_methods_len).await?;
    skip_exact(reader, &mut consumed, compression_methods_len[0] as usize).await?;

    let mut extensions_len = [0u8; 2];
    match reader.read_exact(&mut extensions_len).await {
        Ok(_) => consumed.extend_from_slice(&extensions_len),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
            return Ok(Some(InboundClientHello {
                consumed,
                ..Default::default()
            }));
        }
        Err(error) => return Err(error),
    }

    let extensions_len = u16::from_be_bytes(extensions_len).min(8192) as usize;
    let mut extensions = vec![0u8; extensions_len];
    read_exact(reader, &mut consumed, &mut extensions).await?;

    Ok(Some(parse_extensions(&extensions, consumed)))
}

pub async fn connect_tls_upstream_with_profile<P>(
    socket: TokioSocket,
    tls: &P,
    base_dir: Option<&Path>,
    default_server_name: &str,
) -> Result<TcpRelayStream, RuntimeError>
where
    P: ClientTlsProfile + ?Sized,
{
    let server_name = tls.server_name().unwrap_or(default_server_name).to_owned();

    let mut roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(path) = tls.ca_cert_path() {
        for cert in load_certs(&resolve_path(base_dir, path))? {
            roots
                .add(cert)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        }
    }

    // Look up fingerprint preset for builder-time configuration
    let fingerprint = tls.client_fingerprint().and_then(|name| {
        let fp = crate::fingerprint::lookup_fingerprint(name);
        if fp.is_none() {
            tracing::warn!(fingerprint = %name, "unknown tls fingerprint preset, using defaults");
        }
        fp
    });

    // Build with optional fingerprint via custom CryptoProvider
    let config_base = if let Some(ref fp) = fingerprint {
        let provider = Arc::new(crate::fingerprint::build_provider(fp));
        tracing::debug!(
            fingerprint = %tls.client_fingerprint().unwrap_or(""),
            cipher_count = fp.cipher_suites.len(),
            "tls fingerprint applied"
        );
        ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
    } else {
        ClientConfig::builder()
    };

    let mut config = if tls.insecure() {
        config_base
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(InsecureCertVerifier))
            .with_no_client_auth()
    } else {
        config_base
            .with_root_certificates(roots)
            .with_no_client_auth()
    };

    if tls.disable_sni() {
        config.enable_sni = false;
    }

    // ALPN: use explicit config if provided, otherwise use fingerprint-suggested ALPN
    if !tls.alpn().is_empty() {
        config.alpn_protocols = tls
            .alpn()
            .iter()
            .map(|proto| proto.as_bytes().to_vec())
            .collect();
    } else if fingerprint.is_some() && config.alpn_protocols.is_empty() {
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    }

    let server_name_str = server_name.clone();

    let connector = TlsConnector::from(Arc::new(config));
    let server_name = rustls::pki_types::ServerName::try_from(server_name.as_str())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid tls server_name"))?
        .to_owned();

    let peer_addr = socket.peer_addr().ok();
    tracing::debug!(
        sni = %server_name_str,
        peer = ?peer_addr,
        insecure = tls.insecure(),
        alpn = ?tls.alpn(),
        "tls connecting"
    );

    let stream = connector
        .connect(server_name, socket.into_inner())
        .await
        .map_err(|e| {
            tracing::warn!(
                error = %e,
                sni = %server_name_str,
                peer = ?peer_addr,
                "tls handshake failed"
            );
            e
        })?;

    Ok(TcpRelayStream::new(stream))
}

pub async fn connect_tls_upstream<T>(
    socket: TokioSocket,
    tls: &T,
    base_dir: Option<&Path>,
    default_server_name: &str,
) -> Result<TcpRelayStream, RuntimeError>
where
    T: ClientTlsProfile + ?Sized,
{
    connect_tls_upstream_with_profile(socket, tls, base_dir, default_server_name).await
}

pub async fn connect_tls_stream_with_profile<S, P>(
    stream: S,
    tls: &P,
    base_dir: Option<&Path>,
    default_server_name: &str,
) -> Result<TcpRelayStream, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    P: ClientTlsProfile + ?Sized,
{
    let server_name = tls.server_name().unwrap_or(default_server_name).to_owned();

    let mut roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(path) = tls.ca_cert_path() {
        for cert in load_certs(&resolve_path(base_dir, path))? {
            roots
                .add(cert)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        }
    }

    let fingerprint = tls
        .client_fingerprint()
        .and_then(crate::fingerprint::lookup_fingerprint);

    let config_base = if let Some(ref fingerprint) = fingerprint {
        let provider = Arc::new(crate::fingerprint::build_provider(fingerprint));
        ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?
    } else {
        ClientConfig::builder()
    };

    let mut config = if tls.insecure() {
        config_base
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(InsecureCertVerifier))
            .with_no_client_auth()
    } else {
        config_base
            .with_root_certificates(roots)
            .with_no_client_auth()
    };

    if tls.disable_sni() {
        config.enable_sni = false;
    }

    if !tls.alpn().is_empty() {
        config.alpn_protocols = tls
            .alpn()
            .iter()
            .map(|proto| proto.as_bytes().to_vec())
            .collect();
    } else if fingerprint.is_some() {
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    }

    let connector = TlsConnector::from(Arc::new(config));
    let server_name = rustls::pki_types::ServerName::try_from(server_name.as_str())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid tls server_name"))?
        .to_owned();

    let stream = connector.connect(server_name, stream).await?;

    Ok(TcpRelayStream::new(stream))
}

pub async fn connect_tls_stream<S, T>(
    stream: S,
    tls: &T,
    base_dir: Option<&Path>,
    default_server_name: &str,
) -> Result<TcpRelayStream, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    T: ClientTlsProfile + ?Sized,
{
    connect_tls_stream_with_profile(stream, tls, base_dir, default_server_name).await
}
