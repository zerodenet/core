use std::io;

use crate::RuntimeError;
use tokio::io::{AsyncRead, AsyncWrite};
use zero_platform_tokio::{TcpRelayStream, TokioSocket};

pub(super) async fn connect_stream<S>(
    stream: S,
    server_name: &str,
    fingerprint: &crate::fingerprint::TlsFingerprint,
) -> Result<TcpRelayStream, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    let config = tls13_config(server_name, fingerprint);
    let tls_stream = ztls::stream::Tls13Stream::connect_async(stream, config)
        .await
        .map_err(|error| {
            RuntimeError::Io(io::Error::other(format!(
                "custom TLS handshake over relay stream: {error}"
            )))
        })?;
    Ok(TcpRelayStream::new(tls_stream))
}

pub(super) async fn connect_upstream(
    socket: TokioSocket,
    server_name: &str,
    fingerprint: &crate::fingerprint::TlsFingerprint,
) -> Result<TcpRelayStream, RuntimeError> {
    let config = tls13_config(server_name, fingerprint);
    let tls_stream = ztls::stream::Tls13Stream::connect(socket.into_inner(), config)
        .await
        .map_err(|error| {
            RuntimeError::Io(io::Error::other(format!("custom TLS handshake: {error}")))
        })?;
    Ok(TcpRelayStream::new(tls_stream))
}

fn tls13_config(
    server_name: &str,
    fingerprint: &crate::fingerprint::TlsFingerprint,
) -> ztls::handshake::Tls13Config {
    let cipher_suites: Vec<_> = fingerprint
        .cipher_suites
        .iter()
        .filter_map(|suite| rustls_to_ztls_suite(suite.suite().as_str()?))
        .collect();
    let cipher_suites = if cipher_suites.is_empty() {
        ztls::cipher::DEFAULT_CIPHER_SUITES.to_vec()
    } else {
        cipher_suites
    };
    ztls::handshake::Tls13Config {
        server_name: server_name.to_owned(),
        cipher_suites,
        alpn_protocols: vec!["h2".to_owned(), "http/1.1".to_owned()],
        client_hello_profile: fingerprint.client_hello_profile,
        handshake_timeout_ms: 15_000,
        ..ztls::handshake::Tls13Config::default()
    }
}

fn rustls_to_ztls_suite(name: &str) -> Option<ztls::cipher::CipherSuite> {
    let ztls_name = match name {
        "TLS13_AES_128_GCM_SHA256" => "TLS_AES_128_GCM_SHA256",
        "TLS13_AES_256_GCM_SHA384" => "TLS_AES_256_GCM_SHA384",
        "TLS13_CHACHA20_POLY1305_SHA256" => "TLS_CHACHA20_POLY1305_SHA256",
        "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256" => "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256" => "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
        _ => return None,
    };
    ztls::cipher::CipherSuite::from_name(ztls_name)
}
