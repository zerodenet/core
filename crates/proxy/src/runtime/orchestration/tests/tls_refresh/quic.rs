//! Actual standard QUIC Source consumes the same certificate material owner.
use super::*;
use rustls::pki_types::CertificateDer;
async fn peer(endpoint: &quinn::Endpoint, address: std::net::SocketAddr) -> Vec<u8> {
    let connection = endpoint
        .connect(address, "localhost")
        .unwrap()
        .await
        .unwrap();
    let identity = connection.peer_identity().unwrap();
    let chain = identity.downcast::<Vec<CertificateDer<'static>>>().unwrap();
    let leaf = chain[0].to_vec();
    connection.close(0u32.into(), b"material observed");
    endpoint.wait_idle().await;
    leaf
}
#[tokio::test]
async fn quic_certificate_rotation_keeps_both_backends_and_the_actual_metadata_scope() {
    timeout(Duration::from_secs(30),async {
        for backend in ["rustls","openssl"] {
            let first=rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();let second=rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let directory=tempfile::tempdir().unwrap();let cert=directory.path().join("cert.pem");let key=directory.path().join("key.pem");
            std::fs::write(&cert,first.cert.pem()).unwrap();std::fs::write(&key,first.signing_key.serialize_pem()).unwrap();
            let (proxy,running,address)=fixture::start(json!({"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"vless","users":[],"quic":{"cert_path":cert,"key_path":key,"server_options":{"backend":backend,"reload_interval_secs":1}}}}],"route":{"final":{"type":"direct"}}})).await;
            let original=owner(&proxy,"entry").await;let scope=original.original_scope_for_test();
            let (mut config,_)=fixture::client(vec![first.cert.der().clone(),second.cert.der().clone()]);config.alpn_protocols=vec![b"h3".to_vec()];
            let crypto=quinn::crypto::rustls::QuicClientConfig::try_from(config).unwrap();let mut client=quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();client.set_default_client_config(quinn::ClientConfig::new(Arc::new(crypto)));
            assert_eq!(peer(&client,address).await,first.cert.der().as_ref());
            std::fs::write(&cert,second.cert.pem()).unwrap();std::fs::write(&key,second.signing_key.serialize_pem()).unwrap();
            timeout(Duration::from_secs(5),async{loop{if peer(&client,address).await==second.cert.der().as_ref(){break;}tokio::time::sleep(Duration::from_millis(20)).await;}}).await.unwrap();
            client.close(0u32.into(),b"fixture ended");client.wait_idle().await;running.shutdown().await.unwrap();assert!(!scope.resource_open());closed(&original);
        }
    }).await.unwrap();
}
