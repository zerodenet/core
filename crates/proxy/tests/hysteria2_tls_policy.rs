#![cfg(all(feature = "socks5", feature = "hysteria2"))]
mod support;

use base64::Engine as _;
use ring::digest::{digest, SHA256};
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

#[tokio::test]
async fn hysteria2_node_salamander_hopping_ca_and_pin_relay_tcp() {
    timeout(Duration::from_secs(15), async {
        let material = TempMaterial::new("hysteria2-node-security");
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_path = material.path("cert.pem");
        let key_path = material.path("key.pem");
        std::fs::write(&cert_path, certificate.cert.pem()).unwrap();
        std::fs::write(&key_path, certificate.signing_key.serialize_pem()).unwrap();
        let pin = base64::engine::general_purpose::STANDARD
            .encode(digest(&SHA256, certificate.cert.der().as_ref()).as_ref());

        let hy_port = free_udp_port();
        let socks_port = free_port();
        let config = serde_json::json!({
            "inbounds": [
                {"tag": "hy-in", "listen": {"address": "127.0.0.1", "port": hy_port},
                 "protocol": {
                    "type": "hysteria2",
                    "password": "test-password",
                    "cert_path": cert_path,
                    "key_path": key_path,
                    "transport": {
                        "obfs": {"type": "salamander", "password": "mask-password"}
                    }
                 }},
                {"tag": "socks-in", "listen": {"address": "127.0.0.1", "port": socks_port},
                 "protocol": {"type": "socks5"}}
            ],
            "outbounds": [
                {"tag": "hy-out", "protocol": {
                    "type": "hysteria2",
                    "server": "127.0.0.1",
                    "server_name": "localhost",
                    "port": hy_port,
                    "password": "test-password",
                    "ca_cert_path": cert_path,
                    "tls_options": {"pinned_peer_cert_sha256": [pin]},
                    "transport": {
                        "obfs": {"type": "salamander", "password": "mask-password"},
                        "udp_hop": {
                            "ports": [hy_port],
                            "interval_min_secs": 5,
                            "interval_max_secs": 5
                        }
                    }
                }},
                {"tag": "direct", "protocol": {"type": "direct"}}
            ],
            "route": {
                "rules": [{
                    "condition": {"type": "inbound", "values": ["hy-in"]},
                    "action": {"type": "route", "outbound": "direct"}
                }],
                "final": {"type": "route", "outbound": "hy-out"}
            }
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
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(reply[1], 0);

        let (mut upstream, _) = target.accept().await.unwrap();
        client.write_all(b"secured").await.unwrap();
        let mut payload = [0; 7];
        upstream.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload, b"secured");
        upstream.write_all(b"relayed").await.unwrap();
        let mut response = [0; 7];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"relayed");

        proxy.shutdown().await.unwrap();
    })
    .await
    .expect("Hysteria2 node-security roundtrip timed out");
}

#[tokio::test]
async fn hysteria2_node_rejects_wrong_ca() {
    hysteria2_node_identity_rejection(true).await;
}

#[tokio::test]
async fn hysteria2_node_rejects_wrong_pin() {
    hysteria2_node_identity_rejection(false).await;
}

async fn hysteria2_node_identity_rejection(wrong_ca: bool) {
    timeout(Duration::from_secs(15), async {
        let material = TempMaterial::new("hysteria2-node-identity-rejection");
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let wrong_certificate =
            rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_path = material.path("cert.pem");
        let key_path = material.path("key.pem");
        let wrong_cert_path = material.path("wrong-cert.pem");
        std::fs::write(&cert_path, certificate.cert.pem()).unwrap();
        std::fs::write(&key_path, certificate.signing_key.serialize_pem()).unwrap();
        std::fs::write(&wrong_cert_path, wrong_certificate.cert.pem()).unwrap();
        let wrong_pin = base64::engine::general_purpose::STANDARD
            .encode(digest(&SHA256, wrong_certificate.cert.der().as_ref()).as_ref());
        let hy_port = free_udp_port();
        let socks_port = free_port();
        let mut outbound = serde_json::json!({
            "type": "hysteria2",
            "server": "127.0.0.1",
            "server_name": "localhost",
            "port": hy_port,
            "password": "test-password"
        });
        if wrong_ca {
            outbound["ca_cert_path"] = serde_json::json!(wrong_cert_path);
        } else {
            outbound["ca_cert_path"] = serde_json::json!(cert_path);
            outbound["tls_options"] = serde_json::json!({
                "pinned_peer_cert_sha256": [wrong_pin]
            });
        }
        let config = serde_json::json!({
            "inbounds": [
                {"tag": "hy-in", "listen": {"address": "127.0.0.1", "port": hy_port},
                 "protocol": {"type": "hysteria2", "password": "test-password", "cert_path": cert_path, "key_path": key_path}},
                {"tag": "socks-in", "listen": {"address": "127.0.0.1", "port": socks_port},
                 "protocol": {"type": "socks5"}}
            ],
            "outbounds": [
                {"tag": "hy-out", "protocol": outbound},
                {"tag": "direct", "protocol": {"type": "direct"}}
            ],
            "route": {
                "rules": [{
                    "condition": {"type": "inbound", "values": ["hy-in"]},
                    "action": {"type": "route", "outbound": "direct"}
                }],
                "final": {"type": "route", "outbound": "hy-out"}
            }
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
        client.read_exact(&mut reply).await.unwrap();
        assert_ne!(reply[1], 0, "wrong node identity must be rejected");
        assert!(timeout(Duration::from_millis(100), target.accept())
            .await
            .is_err());
        proxy.shutdown().await.unwrap();
    })
    .await
    .expect("Hysteria2 node identity rejection timed out");
}
