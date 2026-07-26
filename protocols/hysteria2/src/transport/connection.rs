use std::io;
use std::sync::Arc;

use zero_transport::RuntimeError;

use super::{Hysteria2QuicProfile, QuicConnectionOptions};

impl Hysteria2QuicProfile {
    pub fn from_parts(client_fingerprint: Option<&str>) -> Self {
        Self {
            client_fingerprint: client_fingerprint.map(ToOwned::to_owned),
        }
    }

    pub(super) fn client_fingerprint(&self) -> Option<&str> {
        self.client_fingerprint.as_deref()
    }
}

pub async fn open_quic_connection(
    options: QuicConnectionOptions<'_>,
) -> Result<quinn::Connection, RuntimeError> {
    let config_base = if let Some(name) = options.quic_profile.client_fingerprint() {
        if let Some(preset) = zero_transport::fingerprint::lookup_fingerprint(name) {
            let provider = Arc::new(zero_transport::fingerprint::build_provider(&preset));
            tracing::debug!(fingerprint = %name, "quic tls fingerprint applied");
            rustls::ClientConfig::builder_with_provider(provider)
                .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
                .map_err(|error| {
                    RuntimeError::Io(io::Error::other(format!("quic tls protocol: {error}")))
                })?
        } else {
            tracing::warn!(fingerprint = %name, "unknown quic tls fingerprint, using defaults");
            rustls::ClientConfig::builder()
        }
    } else {
        rustls::ClientConfig::builder()
    };

    let mut tls_config = config_base
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipVerify))
        .with_no_client_auth();
    tls_config.alpn_protocols = options.alpn;
    let quic_config = quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)
        .map_err(|error| RuntimeError::Io(io::Error::other(format!("quic tls cfg: {error}"))))?;

    let mut client_config = quinn::ClientConfig::new(Arc::new(quic_config));
    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(std::time::Duration::from_secs(30).try_into().unwrap()));
    transport.datagram_receive_buffer_size(options.datagram_receive_buffer_size);
    client_config.transport_config(Arc::new(transport));

    let bind_addr: std::net::SocketAddr = "0.0.0.0:0"
        .parse()
        .map_err(|error| RuntimeError::Io(io::Error::other(format!("quic bind addr: {error}"))))?;
    let socket = std::net::UdpSocket::bind(bind_addr).map_err(|error| {
        RuntimeError::Io(io::Error::other(format!("quic bind socket: {error}")))
    })?;
    let mut endpoint = quinn::Endpoint::new(
        quinn::EndpointConfig::default(),
        None,
        socket,
        Arc::new(quinn::TokioRuntime),
    )
    .map_err(|error| RuntimeError::Io(io::Error::other(format!("quic endpoint: {error}"))))?;
    endpoint.set_default_client_config(client_config);

    let server_addr = format!("{}:{}", options.server, options.port)
        .parse::<std::net::SocketAddr>()
        .map_err(|error| RuntimeError::Io(io::Error::other(format!("quic addr: {error}"))))?;
    endpoint
        .connect(server_addr, options.server)
        .map_err(|error| RuntimeError::Io(io::Error::other(format!("quic connect: {error}"))))?
        .await
        .map_err(|error| RuntimeError::Io(io::Error::other(format!("quic connection: {error}"))))
}

pub fn negotiated_alpn(connection: &quinn::Connection) -> Option<Vec<u8>> {
    connection
        .handshake_data()?
        .downcast::<quinn::crypto::rustls::HandshakeData>()
        .ok()?
        .protocol
}

#[derive(Debug)]
struct SkipVerify;

impl rustls::client::danger::ServerCertVerifier for SkipVerify {
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
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
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
