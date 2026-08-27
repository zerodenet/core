use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
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

    let server_addrs = options
        .socket_factory
        .resolve_server_addresses(options.server, options.port)
        .await
        .map_err(|error| {
            RuntimeError::Io(io::Error::new(
                error.kind(),
                format!("quic resolve {}:{}: {error}", options.server, options.port),
            ))
        })?;
    let mut last_error = None;

    for server_addr in server_addrs {
        let bind_addr = wildcard_bind_addr(server_addr);
        let socket = match options.socket_factory.bind_std(server_addr) {
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

        let connecting = match endpoint.connect(server_addr, options.server) {
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
        "quic connection to {}:{} failed: {}",
        options.server,
        options.port,
        last_error.unwrap_or_else(|| "no resolved address was connectable".to_owned())
    ))))
}

fn wildcard_bind_addr(server_addr: SocketAddr) -> SocketAddr {
    match server_addr {
        SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        SocketAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
    }
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
