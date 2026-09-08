#![cfg(all(feature = "socks5", feature = "hysteria2"))]
mod support;

use support::interop::TempMaterial;
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{timeout, Duration};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;

async fn roundtrip(insecure: bool) {
    let material = TempMaterial::new("hysteria2-tls-policy");
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_path = material.path("cert.pem");
    let key_path = material.path("key.pem");
    std::fs::write(&cert_path, certificate.cert.pem()).unwrap();
    std::fs::write(&key_path, certificate.signing_key.serialize_pem()).unwrap();
    let hy_port = free_udp_port();
    let socks_port = free_port();
    let config = serde_json::json!({
        "inbounds": [
            {"tag": "hy-in", "listen": {"address": "127.0.0.1", "port": hy_port},
             "protocol": {"type": "hysteria2", "password": "test-password", "cert_path": cert_path, "key_path": key_path}},
            {"tag": "socks-in", "listen": {"address": "127.0.0.1", "port": socks_port}, "protocol": {"type": "socks5"}}
        ],
        "outbounds": [
            {"tag": "hy-out", "protocol": {"type": "hysteria2", "server": "127.0.0.1", "port": hy_port, "password": "test-password", "insecure": insecure}},
            {"tag": "direct", "protocol": {"type": "direct"}}
        ],
        "route": {"rules": [{"condition": {"type": "inbound", "values": ["hy-in"]}, "action": {"type": "route", "outbound": "direct"}}],
                  "final": {"type": "route", "outbound": "hy-out"}}
    });
    let proxy =
        spawn_engine(Proxy::new(RuntimeConfig::parse(&config.to_string()).unwrap()).unwrap());
    wait_for_listener(socks_port).await;
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target_port = target.local_addr().unwrap().port();
    let mut client = TcpStream::connect(("127.0.0.1", socks_port)).await.unwrap();
    client.write_all(&[5, 1, 0]).await.unwrap();
    let mut greeting = [0; 2];
    client.read_exact(&mut greeting).await.unwrap();
    assert_eq!(greeting, [5, 0]);
    let [hi, lo] = target_port.to_be_bytes();
    client
        .write_all(&[5, 1, 0, 1, 127, 0, 0, 1, hi, lo])
        .await
        .unwrap();
    let mut reply = [0; 10];
    timeout(Duration::from_secs(5), client.read_exact(&mut reply))
        .await
        .unwrap()
        .unwrap();
    if insecure {
        assert_eq!(reply[1], 0);
        let (mut upstream, _) = timeout(Duration::from_secs(5), target.accept())
            .await
            .unwrap()
            .unwrap();
        client.write_all(b"request").await.unwrap();
        let mut payload = [0; 7];
        upstream.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload, b"request");
        upstream.write_all(b"response").await.unwrap();
        let mut response = [0; 8];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"response");
    } else {
        assert_ne!(
            reply[1], 0,
            "untrusted certificate must fail at the configured HY2 outbound"
        );
        assert!(timeout(Duration::from_millis(100), target.accept())
            .await
            .is_err());
    }
    drop(client);
    proxy.shutdown().await.unwrap();
}

#[tokio::test]
async fn configured_hysteria2_insecure_false_rejects_untrusted_peer() {
    timeout(Duration::from_secs(10), roundtrip(false))
        .await
        .unwrap();
}

#[tokio::test]
async fn configured_hysteria2_insecure_true_allows_self_signed_peer() {
    timeout(Duration::from_secs(10), roundtrip(true))
        .await
        .unwrap();
}
