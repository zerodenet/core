#![cfg(feature = "tls")]
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_traits::AsyncSocket;
use zero_transport::{profile::OwnedClientTlsProfile, tls};

async fn pair_coalesced(
    initial: &[u8],
    version: &'static rustls::SupportedProtocolVersion,
) -> (
    zero_platform_tokio::TcpRelayStream,
    tls::InboundTlsStream<tokio::io::DuplexStream>,
) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[version])
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![cert.cert.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into(),
    )
    .unwrap();
    let acceptor = tls::TlsAcceptor::from(Arc::new(config));
    let (a, b) = tokio::io::duplex(256 * 1024);
    let profile = OwnedClientTlsProfile {
        options: Default::default(),
        server_name: Some("localhost".into()),
        disable_sni: false,
        ca_cert_path: None,
        insecure: true,
        alpn: vec![],
        client_fingerprint: None,
    };
    let (client, server) = tokio::join!(
        async {
            let mut client = tls::connect_tls_stream(a, &profile, None, "localhost").await?;
            AsyncSocket::write_all(&mut client, initial).await?;
            Ok::<_, zero_transport::RuntimeError>(client)
        },
        tls::accept_tls_handshake(&acceptor, b)
    );
    (client.unwrap(), server.unwrap())
}

async fn pair(
    version: &'static rustls::SupportedProtocolVersion,
) -> (
    zero_platform_tokio::TcpRelayStream,
    tls::InboundTlsStream<tokio::io::DuplexStream>,
) {
    pair_coalesced(&[], version).await
}

#[tokio::test]
async fn handshake_coalesced_with_partial_application_record_keeps_tls_framing() {
    let payload = vec![43; 60001];
    let (mut client, mut server) = pair_coalesced(&payload, &rustls::version::TLS13).await;
    let mut received = vec![0; payload.len()];
    server.read_exact(&mut received).await.unwrap();
    assert_eq!(received, payload);
    AsyncSocket::write_all(&mut client, b"next record")
        .await
        .unwrap();
    let mut next = [0; 11];
    server.read_exact(&mut next).await.unwrap();
    assert_eq!(&next, b"next record");
}

#[tokio::test]
async fn tls13_handoff_preserves_buffered_plaintext_and_coalesced_raw_records() {
    let (mut client, mut server) = pair(&rustls::version::TLS13).await;
    let c = client.transport_bypass_control().unwrap();
    let s = server.transport_bypass_control().unwrap();
    let large = vec![77; 65537];
    AsyncSocket::write_all(&mut client, &large).await.unwrap();
    let mut received = vec![0; large.len()];
    server.read_exact(&mut received).await.unwrap();
    assert_eq!(received, large);
    // Queue an encrypted record followed immediately by raw TLS-shaped records.
    // Deliberately leave plaintext unread when requesting the inbound transition.
    AsyncWriteExt::write_all(&mut client, b"transition-and-tail")
        .await
        .unwrap();
    c.request_write_bypass();
    let raw = [vec![23, 3, 3, 0, 6], b"opaque".to_vec()].concat();
    AsyncWriteExt::write_all(&mut client, &raw).await.unwrap();
    client.flush().await.unwrap();
    let mut prefix = [0; 10];
    server.read_exact(&mut prefix).await.unwrap();
    assert_eq!(&prefix, b"transition");
    s.request_read_bypass();
    let mut tail = [0; 9];
    server.read_exact(&mut tail).await.unwrap();
    assert_eq!(&tail, b"-and-tail");
    let mut output = vec![0; raw.len()];
    server.read_exact(&mut output).await.unwrap();
    assert_eq!(output, raw);
    AsyncWriteExt::write_all(&mut server, b"response")
        .await
        .unwrap();
    s.request_write_bypass();
    AsyncWriteExt::write_all(&mut server, &raw).await.unwrap();
    server.flush().await.unwrap();
    let mut response = [0; 8];
    client.read_exact(&mut response).await.unwrap();
    assert_eq!(&response, b"response");
    c.request_read_bypass();
    client.read_exact(&mut output).await.unwrap();
    assert_eq!(output, raw);
}

#[tokio::test]
async fn tls12_does_not_advertise_a_raw_handoff() {
    let (mut client, mut server) = pair(&rustls::version::TLS12).await;
    assert!(client.transport_bypass_control().is_none());
    assert!(server.transport_bypass_control().is_none());
    AsyncSocket::write_all(&mut client, b"ordinary TLS")
        .await
        .unwrap();
    let mut bytes = [0; 12];
    server.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"ordinary TLS");
}
