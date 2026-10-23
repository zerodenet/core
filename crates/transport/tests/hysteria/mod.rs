use super::*;
use bytes::Bytes;
use tokio::time::{timeout, Duration};
mod masquerade;
mod progress;
mod proxy;
fn endpoint() -> (quinn::Endpoint, Profile) {
    let profile = Profile::from_options(OptionsRef {
        auth: "secret",
        uplink_bytes_per_sec: 1_000_000,
        downlink_bytes_per_sec: 2_000_000,
        ..Default::default()
    })
    .unwrap();
    endpoint_with_profile(profile)
}
fn endpoint_with_profile(profile: Profile) -> (quinn::Endpoint, Profile) {
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
    use std::sync::atomic::{AtomicU8, Ordering};
    let phases = Arc::new([AtomicU8::new(0), AtomicU8::new(0)]);
    let observed = phases.clone();
    let result = timeout(Duration::from_secs(10), async move {
        let (endpoint, profile) = endpoint();
        let address = endpoint.local_addr().unwrap();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let server_phases = phases.clone();
        let server = tokio::spawn(async move {
            server_phases[1].store(1, Ordering::Relaxed);
            let connection = endpoint.accept().await.unwrap().await.unwrap();
            let incoming = accept_connection(connection, &profile);
            server_phases[1].store(2, Ordering::Relaxed);
            assert!(timeout(Duration::from_millis(100), incoming.accept())
                .await
                .is_err());
            ready_tx.send(()).unwrap();
            server_phases[1].store(3, Ordering::Relaxed);
            let mut stream = incoming.accept().await.unwrap();
            server_phases[1].store(4, Ordering::Relaxed);
            let mut payload = [0u8; 4];
            tokio::io::AsyncReadExt::read_exact(&mut stream, &mut payload)
                .await
                .unwrap();
            assert_eq!(&payload, b"ping");
            server_phases[1].store(5, Ordering::Relaxed);
            tokio::io::AsyncWriteExt::write_all(&mut stream, b"pong")
                .await
                .unwrap();
            server_phases[1].store(6, Ordering::Relaxed);
            tokio::io::AsyncWriteExt::shutdown(&mut stream)
                .await
                .unwrap();
            server_phases[1].store(7, Ordering::Relaxed);
            // Keep the carrier alive until the acknowledged application response is consumed.
            incoming.connection.closed().await;
            server_phases[1].store(8, Ordering::Relaxed);
        });
        let profile = Profile::from_options(OptionsRef {
            auth: "secret",
            uplink_bytes_per_sec: 3_000_000,
            downlink_bytes_per_sec: 4_000_000,
            ..Default::default()
        })
        .unwrap();
        phases[0].store(1, Ordering::Relaxed);
        let connection = dial(address, &profile).await;
        phases[0].store(2, Ordering::Relaxed);
        let (mut send, mut recv) = connection.open_bi().await.unwrap();
        send.write_all(&[0x44, 0x01, 0]).await.unwrap();
        phases[0].store(3, Ordering::Relaxed);
        assert!(recv.read(&mut [0u8; 1]).await.is_err());
        phases[0].store(4, Ordering::Relaxed);
        ready_rx.await.unwrap();
        phases[0].store(5, Ordering::Relaxed);
        let client = Client::authenticate(connection.clone(), &profile)
            .await
            .unwrap();
        phases[0].store(6, Ordering::Relaxed);
        assert_eq!(connection.congestion_state().pacing_rate(), Some(2_000_000));
        let mut stream = client.open().await.unwrap();
        phases[0].store(7, Ordering::Relaxed);
        tokio::io::AsyncWriteExt::write_all(&mut stream, b"ping")
            .await
            .unwrap();
        phases[0].store(8, Ordering::Relaxed);
        let mut body = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut body)
            .await
            .unwrap();
        assert_eq!(body, b"pong");
        phases[0].store(9, Ordering::Relaxed);
        drop(stream);
        drop(client);
        server.await.unwrap();
        phases[0].store(10, Ordering::Relaxed);
    })
    .await;
    const CLIENT: [&str; 11] = [
        "initial",
        "dial",
        "open unauthenticated stream",
        "reject unauthenticated stream",
        "server gate",
        "authenticate",
        "open authenticated stream",
        "write ping",
        "read pong and EOF",
        "close carrier and join server",
        "complete",
    ];
    const SERVER: [&str; 9] = [
        "initial",
        "accept carrier",
        "gate unauthenticated streams",
        "accept authenticated stream",
        "read ping",
        "write pong",
        "finish stream and await acknowledgment",
        "wait carrier close",
        "complete",
    ];
    assert!(
        result.is_ok(),
        "10s authentication deadline: client={}, server={}",
        CLIENT[observed[0].load(Ordering::Relaxed) as usize],
        SERVER[observed[1].load(Ordering::Relaxed) as usize]
    );
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
