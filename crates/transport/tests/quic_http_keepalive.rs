#![cfg(feature = "quic")]
use std::{sync::Arc, time::Duration};
use zero_transport::quic::{client_config_with_ca, set_http_keepalive};

#[tokio::test]
async fn http3_default_sends_ping_at_ten_seconds_and_nonzero_http_option_disables_it() {
    let cert = rcgen::generate_simple_self_signed(vec!["keepalive.test".into()]).unwrap();
    let path = std::env::temp_dir().join(format!("zero-h3-keepalive-{}.pem", std::process::id()));
    std::fs::write(&path, cert.cert.pem()).unwrap();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let tls = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![cert.cert.der().clone()], key.into())
    .unwrap();
    let config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
    ));
    let server = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut connections = Vec::new();
    for option in [0, 1, -1] {
        let mut config = client_config_with_ca(false, None, &[], None, Some(&path)).unwrap();
        set_http_keepalive(&mut config, option);
        let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(config);
        let outgoing = endpoint
            .connect(server.local_addr().unwrap(), "keepalive.test")
            .unwrap();
        let (outgoing, incoming) = tokio::join!(outgoing, async {
            server.accept().await.unwrap().await.unwrap()
        });
        connections.push((option, endpoint, outgoing.unwrap(), incoming));
    }
    // Allow handshake and path-MTU probes to settle before counting heartbeats.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let before: Vec<_> = connections
        .iter()
        .map(|(_, _, c, _)| c.stats().frame_tx.ping)
        .collect();
    tokio::time::sleep(Duration::from_secs(10)).await;
    for ((option, endpoint, outgoing, incoming), before) in connections.into_iter().zip(before) {
        let sent = outgoing.stats().frame_tx.ping - before;
        assert_eq!(
            sent > 0,
            option == 0,
            "HTTP keepalive option={option}, heartbeat count={sent}"
        );
        assert!(outgoing.close_reason().is_none());
        outgoing.close(0u32.into(), b"done");
        incoming.close(0u32.into(), b"done");
        endpoint.close(0u32.into(), b"done");
    }
    server.close(0u32.into(), b"done");
    std::fs::remove_file(path).unwrap();
}
