//! Shared QUIC client TLS and endpoint construction. Protocols select policy.
use crate::RuntimeError;
use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
};

pub fn client_config(
    insecure: bool,
    fingerprint: Option<&str>,
    alpn: &[Vec<u8>],
    datagram_receive_buffer_size: Option<usize>,
) -> Result<quinn::ClientConfig, RuntimeError> {
    let provider = Arc::new(
        fingerprint
            .and_then(crate::fingerprint::lookup_fingerprint)
            .map(|preset| crate::fingerprint::build_client_provider(&preset))
            .unwrap_or_else(rustls::crypto::ring::default_provider),
    );
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(io::Error::other)?;
    let mut tls = if insecure {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(InsecureServerVerifier { provider }))
            .with_no_client_auth()
    } else {
        let roots =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        builder.with_root_certificates(roots).with_no_client_auth()
    };
    tls.alpn_protocols = alpn.to_vec();
    let crypto =
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).map_err(io::Error::other)?;
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(std::time::Duration::from_secs(30).try_into().unwrap()));
    transport.datagram_receive_buffer_size(datagram_receive_buffer_size);
    config.transport_config(Arc::new(transport));
    Ok(config)
}

/// Disabling trust/name checks does not disable proof of private-key possession.
#[derive(Debug)]
struct InsecureServerVerifier {
    provider: Arc<rustls::crypto::CryptoProvider>,
}
impl rustls::client::danger::ServerCertVerifier for InsecureServerVerifier {
    fn verify_server_cert(
        &self,
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &[rustls::pki_types::CertificateDer<'_>],
        _: &rustls::pki_types::ServerName<'_>,
        _: &[u8],
        _: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

pub async fn connect_quic_endpoint(
    server: &str,
    port: u16,
    server_name: &str,
    client_config: quinn::ClientConfig,
    sockets: &crate::OutboundDatagramSocketFactory,
) -> Result<quinn::Connection, RuntimeError> {
    let server_addrs = sockets
        .resolve_server_addresses(server, port)
        .await
        .map_err(|error| {
            RuntimeError::Io(io::Error::new(
                error.kind(),
                format!("quic resolve {server}:{port}: {error}"),
            ))
        })?;
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

fn wildcard_bind_addr(server_addr: SocketAddr) -> SocketAddr {
    match server_addr {
        SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        SocketAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
    }
}

#[cfg(test)]
#[path = "tests/client.rs"]
mod tests;
