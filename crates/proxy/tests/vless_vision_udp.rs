#![cfg(all(feature = "socks5", feature = "vless"))]
mod support;
use support::interop::{socks5_udp_echo_targets, spawn_udp_echo_count};
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;
const PRIVATE: &str = "OKMOFBeltHBXaTQ8cIcsgabVQcqXeTB9Ih3lPtWMY04";
const PUBLIC: &str = "9AwHi13y1rN6EWTSo8-HNCOhrzr251jNY7SSIxo0diA";
#[tokio::test]
async fn vision_udp_uses_xudp_for_both_outbound_policies_and_multiple_targets() {
    for flow in ["xtls-rprx-vision", "xtls-rprx-vision-udp443"] {
        let server_port = free_port();
        let socks_port = free_port();
        let server = RuntimeConfig::parse(&serde_json::json!({
            "inbounds": [{"tag":"vless", "listen":{"address":"127.0.0.1","port":server_port},
                "protocol":{"type":"vless","users":[{"id":"vision-user","flow":"xtls-rprx-vision"}],
                    "reality":{"private_key":PRIVATE,"short_ids":["0123456789abcdef"],"server_name":"private.test"}}}],
            "route":{"rules":[],"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let client = RuntimeConfig::parse(&serde_json::json!({
            "inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":socks_port},"protocol":{"type":"socks5"}}],
            "outbounds":[{"tag":"out","protocol":{"type":"vless","id":"vision-user","server":"127.0.0.1","port":server_port,"flow":flow,
                "reality":{"public_key":PUBLIC,"short_id":"0123456789abcdef","server_name":"private.test"}}}],
            "route":{"rules":[],"final":{"type":"route","outbound":"out"}}
        }).to_string()).unwrap();
        let server = spawn_engine(Proxy::new(server).unwrap());
        let client = spawn_engine(Proxy::new(client).unwrap());
        wait_for_listener(server_port).await;
        wait_for_listener(socks_port).await;
        let a = free_udp_port();
        let b = free_udp_port();
        let echo_a = spawn_udp_echo_count(a, 2).await;
        let echo_b = spawn_udp_echo_count(b, 1).await;
        let packets: &[(u16, &[u8])] = &[(a, b"first"), (b, b"second"), (a, b"third")];
        let replies = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            socks5_udp_echo_targets(socks_port, packets),
        )
        .await
        .unwrap();
        assert_eq!(
            replies,
            vec![b"first".to_vec(), b"second".to_vec(), b"third".to_vec()]
        );
        echo_a.await.unwrap();
        echo_b.await.unwrap();
        client.shutdown().await.unwrap();
        server.shutdown().await.unwrap();
    }
}
