use super::*;
use bytes::Bytes;
use tokio::time::{timeout, Duration};
mod masquerade;
mod proxy;
fn endpoint() -> (quinn::Endpoint, Profile) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![cert.cert.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into(),
    )
    .unwrap();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let profile = Profile::from_options(OptionsRef {
        auth: "secret",
        uplink_bytes_per_sec: 1_000_000,
        downlink_bytes_per_sec: 2_000_000,
        ..Default::default()
    })
    .unwrap();
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
    ));
    config.transport_config(Arc::new(profile.transport(true).unwrap()));
    (
        quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap(),
        profile,
    )
}
async fn dial(address: std::net::SocketAddr, profile: &Profile) -> quinn::Connection {
    let mut config = crate::quic::client_config(true, None, &[b"h3".to_vec()], None).unwrap();
    config.transport_config(Arc::new(profile.transport(false).unwrap()));
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(config);
    endpoint
        .connect(address, "localhost")
        .unwrap()
        .await
        .unwrap()
}
#[tokio::test]
async fn authentication_gates_native_streams_and_negotiates_bandwidth() {
    timeout(Duration::from_secs(10), async {
        let (endpoint, profile) = endpoint();
        let address = endpoint.local_addr().unwrap();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let connection = endpoint.accept().await.unwrap().await.unwrap();
            let incoming = accept_connection(connection, &profile);
            assert!(timeout(Duration::from_millis(100), incoming.accept())
                .await
                .is_err());
            ready_tx.send(()).unwrap();
            let mut stream = incoming.accept().await.unwrap();
            let mut payload = [0u8; 4];
            tokio::io::AsyncReadExt::read_exact(&mut stream, &mut payload)
                .await
                .unwrap();
            assert_eq!(&payload, b"ping");
            tokio::io::AsyncWriteExt::write_all(&mut stream, b"pong")
                .await
                .unwrap();
            tokio::io::AsyncWriteExt::shutdown(&mut stream)
                .await
                .unwrap();
            // Keep the carrier alive until the acknowledged application response is consumed.
            incoming.connection.closed().await;
        });
        let profile = Profile::from_options(OptionsRef {
            auth: "secret",
            uplink_bytes_per_sec: 3_000_000,
            downlink_bytes_per_sec: 4_000_000,
            ..Default::default()
        })
        .unwrap();
        let connection = dial(address, &profile).await;
        let (mut send, mut recv) = connection.open_bi().await.unwrap();
        send.write_all(&[0x44, 0x01, 0]).await.unwrap();
        assert!(recv.read(&mut [0u8; 1]).await.is_err());
        ready_rx.await.unwrap();
        let client = Client::authenticate(connection.clone(), &profile)
            .await
            .unwrap();
        assert_eq!(connection.congestion_state().pacing_rate(), Some(2_000_000));
        let mut stream = client.open().await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut stream, b"ping")
            .await
            .unwrap();
        let mut body = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut body)
            .await
            .unwrap();
        assert_eq!(body, b"pong");
        drop(stream);
        drop(client);
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn wrong_authentication_receives_masquerade_and_opens_no_carrier() {
    timeout(Duration::from_secs(5), async {
        let (endpoint, profile) = endpoint();
        let address = endpoint.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let connection = endpoint.accept().await.unwrap().await.unwrap();
            let incoming = accept_connection(connection.clone(), &profile);
            connection.closed().await;
            assert!(incoming.receiver.lock().await.try_recv().is_err());
        });
        let wrong = Profile::new("incorrect").unwrap();
        let connection = dial(address, &wrong).await;
        assert!(Client::authenticate(connection, &wrong).await.is_err());
        server.await.unwrap();
    })
    .await
    .unwrap();
}
