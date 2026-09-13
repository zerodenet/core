#![cfg(feature = "quic")]
use std::{sync::Arc, time::Duration};
use zero_transport::quic::client_config_with_ca;
#[tokio::test]
async fn custom_ca_authenticates_peer_without_disabling_name_verification() {
    let cert = rcgen::generate_simple_self_signed(vec!["private.test".into()]).unwrap();
    let path = std::env::temp_dir().join(format!("zero-quic-ca-{}.pem", std::process::id()));
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
    let address = server.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        while let Some(incoming) = server.accept().await {
            tokio::spawn(async move {
                if let Ok(connection) = incoming.await {
                    connection.closed().await;
                }
            });
        }
    });
    for (name, ca, accepted) in [
        ("private.test", Some(path.as_path()), true),
        ("wrong.test", Some(path.as_path()), false),
        ("private.test", None, false),
    ] {
        let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        client
            .set_default_client_config(client_config_with_ca(false, None, &[], None, ca).unwrap());
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            client.connect(address, name).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            result.is_ok(),
            accepted,
            "name={name}, custom_ca={}",
            ca.is_some()
        );
        if let Ok(connection) = result {
            connection.close(0u32.into(), b"done");
        }
    }
    std::fs::write(&path, "not a PEM certificate").unwrap();
    assert!(client_config_with_ca(false, None, &[], None, Some(&path)).is_err());
    std::fs::remove_file(path).unwrap();
    server_task.abort();
}
