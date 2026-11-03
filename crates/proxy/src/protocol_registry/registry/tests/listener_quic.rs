//! Real IPv6 bind and handshake coverage through each QUIC inbound adapter.
#![cfg(any(feature = "hysteria2", feature = "vless"))]

use std::net::{Ipv6Addr, SocketAddr};
use std::time::Duration;

use serde_json::json;

#[tokio::test]
async fn quic_adapters_bind_bare_and_bracketed_ipv6() {
    if let Err(error) = std::net::UdpSocket::bind((Ipv6Addr::LOCALHOST, 0)) {
        eprintln!("SKIP IPv6 QUIC listener test: {error}");
        return;
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let directory = tempfile::tempdir().unwrap();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let cert_path = directory.path().join("cert.pem");
    let key_path = directory.path().join("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
    let mut protocols = Vec::new();
    #[cfg(feature = "hysteria2")]
    protocols.push((
        json!({"type":"hysteria2", "password":"test", "cert_path":cert_path,"key_path":key_path}),
        b"h3".to_vec(),
    ));
    #[cfg(feature = "vless")]
    protocols.push((
        json!({"type":"vless", "users":[], "quic":{"cert_path":cert_path,"key_path":key_path}}),
        b"h3".to_vec(),
    ));
    let registry = crate::register::protocol_registry();
    for (protocol, alpn) in protocols {
        for address in ["::1", "[::1]", "::", "[::]"] {
            let inbound = serde_json::from_value(json!({
                "tag":"quic-v6", "listen":{"address":address,"port":0},
                "protocol":protocol,
            }))
            .unwrap();
            assert!(registry
                .inbounds
                .has_factory_for(protocol["type"].as_str().unwrap(), &inbound));
            standard_quic(inbound, &alpn).await;
        }
    }
}

async fn standard_quic(inbound: zero_config::InboundConfig, alpn: &[u8]) {
    let tag = inbound.tag.clone();
    let config = zero_config::RuntimeConfig::parse(
        &json!({"inbounds":[inbound],
        "route":{"final":{"type":"direct"}}})
        .to_string(),
    )
    .unwrap();
    let proxy = crate::runtime::Proxy::new(config).unwrap();
    let listener =
        crate::runtime::test_listener::start(&proxy, proxy.engine().config().inbounds[0].clone())
            .await;
    assert!(listener.address.is_ipv6());
    let address = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), listener.address.port());
    let mut client =
        quinn::Endpoint::client(SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 0)).unwrap();
    client.set_default_client_config(
        zero_transport::quic::client_config(true, None, &[alpn.to_vec()], None).unwrap(),
    );
    let connected = tokio::time::timeout(
        Duration::from_secs(5),
        client.connect(address, "localhost").unwrap(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(connected.remote_address().is_ipv6());
    connected.close(0u32.into(), b"test complete");
    client.close(0u32.into(), b"test complete");
    client.wait_idle().await;
    crate::runtime::test_listener::close(&proxy, &tag, listener).await;
}
