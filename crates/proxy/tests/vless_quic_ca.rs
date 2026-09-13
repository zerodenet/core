#![cfg(all(feature = "socks5", feature = "vless"))]
mod support;
use serde_json::json;
use support::interop::*;
use support::{free_port, free_udp_port, spawn_engine, wait_for, wait_for_listener};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;

#[tokio::test]
async fn relative_quic_ca_reaches_tcp_and_udp_for_raw_and_http3_carriers() {
    for http3 in [false, true] {
        let material = TempMaterial::new("vless-relative-quic-ca");
        let cert = rcgen::generate_simple_self_signed(vec!["private.test".into()]).unwrap();
        std::fs::write(material.path("ca.pem"), cert.cert.pem()).unwrap();
        std::fs::write(material.path("key.pem"), cert.signing_key.serialize_pem()).unwrap();
        let port = free_udp_port();
        let socks = free_port();
        let mut inbound = json!({"inbounds":[{"tag":"vless","listen":{"address":"127.0.0.1","port":port},"protocol":{"type":"vless","users":[{"id":"private-ca"}],"quic":{"cert_path":"ca.pem","key_path":"key.pem"}}}],"route":{"rules":[],"final":{"type":"direct"}}});
        let mut outbound = json!({"inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":socks},"protocol":{"type":"socks5"}}],"outbounds":[{"tag":"up","protocol":{"type":"vless","server":"127.0.0.1","port":port,"id":"private-ca","quic":{"server_name":"private.test","ca_cert_path":"ca.pem"}}}],"route":{"rules":[],"final":{"type":"route","outbound":"up"}}});
        if http3 {
            inbound["inbounds"][0]["protocol"]["split_http"] = json!({"path":"/ca/","mode":"auto"});
            outbound["outbounds"][0]["protocol"]["split_http"] =
                json!({"path":"/ca/","mode":"stream-up"});
        }
        let inbound_path = material.path("inbound.json");
        let outbound_path = material.path("outbound.json");
        std::fs::write(&inbound_path, inbound.to_string()).unwrap();
        std::fs::write(&outbound_path, outbound.to_string()).unwrap();
        let server = spawn_engine(
            Proxy::new(RuntimeConfig::load_from_path(&inbound_path).unwrap()).unwrap(),
        );
        wait_for("QUIC listener bound", || {
            std::net::UdpSocket::bind(("127.0.0.1", port)).is_err()
        })
        .await;
        let client = spawn_engine(
            Proxy::new(RuntimeConfig::load_from_path(&outbound_path).unwrap()).unwrap(),
        );
        wait_for_listener(socks).await;
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            let echo_port = free_port();
            let data = vec![127; 65537];
            let echo = spawn_tcp_echo(echo_port, data.len()).await;
            assert_eq!(
                socks5_tcp_echo_once(socks, echo_port, &data).await.unwrap(),
                data
            );
            echo.await.unwrap();
            let echo_port = free_udp_port();
            let echo = spawn_udp_echo_count(echo_port, 2).await;
            assert_eq!(
                socks5_udp_echo_targets(socks, &[(echo_port, b"first"), (echo_port, b"second")])
                    .await,
                vec![b"first".to_vec(), b"second".to_vec()]
            );
            echo.await.unwrap();
        })
        .await
        .expect("relative CA is used by both carrier paths");
        client.shutdown().await.unwrap();
        server.shutdown().await.unwrap();
    }
}
