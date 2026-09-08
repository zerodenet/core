use super::*;
use std::sync::Arc;
use tokio::time::{timeout, Duration};

async fn pair(
    insecure: bool,
) -> (
    Result<quinn::Connection, quinn::ConnectionError>,
    Result<quinn::Connection, quinn::ConnectionError>,
) {
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
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
    let server = quinn::Endpoint::server(
        quinn::ServerConfig::with_crypto(Arc::new(crypto)),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    client.set_default_client_config(
        zero_transport::quic::client_config(insecure, None, &[b"h3".to_vec()], Some(65536))
            .unwrap(),
    );
    let connecting = client
        .connect(server.local_addr().unwrap(), "localhost")
        .unwrap();
    let (client, server) = tokio::join!(connecting, async { server.accept().await.unwrap().await });
    (client, server)
}

#[tokio::test]
async fn quic_rejects_untrusted_certificate_by_default() {
    let (client, server) = timeout(Duration::from_secs(5), pair(false)).await.unwrap();
    assert!(
        client.is_err(),
        "a self-signed certificate must not be trusted by default"
    );
    assert!(server.is_err());
}

#[tokio::test]
async fn udp_disabled_response_preserves_tcp_session_and_rejects_udp() {
    timeout(Duration::from_secs(5), async {
        let (client, server) = pair(true).await;
        let (client, server) = (client.unwrap(), server.unwrap());
        let serve = tokio::spawn(async move {
            let mut http = h3::server::builder()
                .build::<_, Bytes>(h3_quinn::Connection::new(server.clone()))
                .await
                .unwrap();
            let (request, mut stream) = http
                .accept()
                .await
                .unwrap()
                .unwrap()
                .resolve_request()
                .await
                .unwrap();
            assert_eq!(request.headers()["hysteria-auth"], "test-password");
            stream
                .send_response(
                    http::Response::builder()
                        .status(233)
                        .header("Hysteria-UDP", "false")
                        .header("Hysteria-CC-RX", "auto")
                        .body(())
                        .unwrap(),
                )
                .await
                .unwrap();
            stream.finish().await.unwrap();
            let (_send, mut recv) = server.accept_bi().await.unwrap();
            assert_eq!(recv.read_to_end(16).await.unwrap(), b"tcp");
            server.closed().await;
            drop(http);
        });
        let authenticated = authenticate_http3(client, "test-password").await.unwrap();
        assert!(!authenticated.negotiated().udp_enabled);
        assert_eq!(
            authenticated.negotiated().receive_bandwidth,
            crate::handshake::ReceiveBandwidth::Auto
        );
        assert!(authenticated.require_udp().is_err());
        let (mut send, _recv) = authenticated.connection().open_bi().await.unwrap();
        send.write_all(b"tcp").await.unwrap();
        send.finish().unwrap();
        send.stopped().await.unwrap();
        drop(authenticated);
        serve.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancellation_during_authentication_closes_quic_session() {
    timeout(Duration::from_secs(5), async {
        let (client, server) = pair(true).await;
        let (client, server) = (client.unwrap(), server.unwrap());
        let auth = tokio::spawn(authenticate_http3(client, "test-password"));
        let mut http = h3::server::builder()
            .build::<_, Bytes>(h3_quinn::Connection::new(server.clone()))
            .await
            .unwrap();
        let (_request, _stream) = http
            .accept()
            .await
            .unwrap()
            .unwrap()
            .resolve_request()
            .await
            .unwrap();
        auth.abort();
        assert!(matches!(auth.await, Err(error) if error.is_cancelled()));
        server.closed().await;
    })
    .await
    .unwrap();
}
