#![cfg(all(feature = "socks5", feature = "vless"))]
mod support;
use base64::Engine;
use serde_json::json;
use support::interop::*;
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};
use vless::encryption::{config::EncryptionConfig, EncryptionServer};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;

fn profiles(mask: &str, chained: bool) -> (String, String) {
    let encode = |bytes: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    let secret = if chained {
        format!(
            "{}.{}.{}",
            encode(&[1; 32]),
            encode(&[2; 64]),
            encode(&[3; 32])
        )
    } else {
        encode(&[2; 64])
    };
    let server = format!("mlkem768x25519plus.{mask}.600s.100-35-35.{secret}");
    let keys = EncryptionServer::new(EncryptionConfig::server(&server).unwrap().unwrap())
        .unwrap()
        .public_keys();
    let client = format!(
        "mlkem768x25519plus.{mask}.0rtt.100-35-35.{}",
        keys.iter()
            .map(|key| encode(key))
            .collect::<Vec<_>>()
            .join(".")
    );
    (client, server)
}
async fn exercise(socks: u16) {
    for round in 0..3u8 {
        let echo = free_port();
        let payload = vec![round; 32769];
        let task = spawn_tcp_echo(echo, payload.len()).await;
        assert_eq!(
            socks5_tcp_echo_once(socks, echo, &payload).await.unwrap(),
            payload
        );
        task.await.unwrap();
    }
    let udp = free_udp_port();
    let task = spawn_udp_echo_count(udp, 3).await;
    let packets: &[(u16, &[u8])] = &[(udp, b"one"), (udp, b"two"), (udp, b"three")];
    assert_eq!(
        socks5_udp_echo_targets(socks, packets).await,
        vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]
    );
    task.await.unwrap();
}
fn zero_server(port: u16, decryption: &str) -> serde_json::Value {
    json!({"inbounds":[{"tag":"server","listen":{"address":"127.0.0.1","port":port},"protocol":{"type":"vless","users":[{"id":"encryption-user"}],"decryption":decryption}}],"route":{"rules":[],"final":{"type":"direct"}}})
}
fn zero_client(port: u16, socks: u16, encryption: &str) -> serde_json::Value {
    json!({"inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":socks},"protocol":{"type":"socks5"}}],"outbounds":[{"tag":"upstream","protocol":{"type":"vless","id":"encryption-user","server":"127.0.0.1","port":port,"encryption":encryption,"xudp_concurrency":2}}],"route":{"rules":[],"final":{"type":"route","outbound":"upstream"}}})
}
#[tokio::test]
async fn native_encryption_tcp_and_xudp_all_masks() {
    for mask in ["native", "xorpub", "random"] {
        let (encryption, decryption) = profiles(mask, true);
        let port = free_port();
        let socks = free_port();
        let server = spawn_engine(
            Proxy::new(RuntimeConfig::parse(&zero_server(port, &decryption).to_string()).unwrap())
                .unwrap(),
        );
        let client = spawn_engine(
            Proxy::new(
                RuntimeConfig::parse(&zero_client(port, socks, &encryption).to_string()).unwrap(),
            )
            .unwrap(),
        );
        wait_for_listener(port).await;
        wait_for_listener(socks).await;
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(20), exercise(socks)).await;
        client.shutdown().await.unwrap();
        server.shutdown().await.unwrap();
        result.unwrap();
    }
}
async fn official(zero_outbound: bool) {
    let binary = std::env::var("XRAY_BIN").expect("official XRAY_BIN v26.3.27 required");
    let output = std::process::Command::new(&binary)
        .arg("version")
        .output()
        .unwrap();
    let version = String::from_utf8_lossy(&output.stdout);
    assert!(version.contains("26.3.27") && version.contains("d2758a0"));
    for mask in ["native", "xorpub", "random"] {
        for chain in [false, true] {
            let (encryption, decryption) = profiles(mask, chain);
            let port = free_port();
            let socks = free_port();
            let material = TempMaterial::new("vless-encryption");
            let (zero, xray) = if zero_outbound {
                (
                    zero_client(port, socks, &encryption),
                    json!({"log":{"loglevel":"warning"},"inbounds":[{"listen":"127.0.0.1","port":port,"protocol":"vless","settings":{"clients":[{"id":"encryption-user"}],"decryption":decryption}}],"outbounds":[{"protocol":"freedom"}]}),
                )
            } else {
                (
                    zero_server(port, &decryption),
                    json!({"log":{"loglevel":"warning"},"inbounds":[{"listen":"127.0.0.1","port":socks,"protocol":"socks","settings":{"udp":true}}],"outbounds":[{"protocol":"vless","settings":{"address":"127.0.0.1","port":port,"id":"encryption-user","encryption":encryption}}]}),
                )
            };
            let zero =
                spawn_engine(Proxy::new(RuntimeConfig::parse(&zero.to_string()).unwrap()).unwrap());
            let path = material.path("xray.json");
            std::fs::write(&path, xray.to_string()).unwrap();
            let mut xray = XrayProcess::start(binary.clone(), &path, &material);
            wait_for_listener(port).await;
            wait_for_listener(socks).await;
            let result =
                tokio::time::timeout(std::time::Duration::from_secs(20), exercise(socks)).await;
            xray.kill();
            zero.shutdown().await.unwrap();
            assert!(
                result.is_ok(),
                "mask={mask}, chain={chain}, zero_outbound={zero_outbound}: {}",
                xray.logs()
            );
        }
    }
}
#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn zero_encryption_to_official() {
    official(true).await;
}
#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn official_encryption_to_zero() {
    official(false).await;
}
