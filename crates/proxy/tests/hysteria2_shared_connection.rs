#![cfg(all(feature = "hysteria2", feature = "socks5"))]
use crate::support;
use support::interop::{init_logs, socks5_udp_echo_sequence, TempMaterial};
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};
use tokio::{
    net::UdpSocket,
    time::{timeout, Duration},
};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;

#[tokio::test]
async fn shared_inbound_udp_sessions_to_same_target_keep_reversed_responses_isolated() {
    same_target_sessions(false).await;
}

#[tokio::test]
async fn shared_hy2_outbound_keeps_same_target_logical_udp_sessions_isolated() {
    same_target_sessions(true).await;
}

async fn same_target_sessions(via_hy2: bool) {
    init_logs("hysteria2=debug");
    timeout(Duration::from_secs(20), async {
        let material = TempMaterial::new("hy2-shared-inbound");
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_path = material.path("cert.pem");
        let key_path = material.path("key.pem");
        std::fs::write(&cert_path, cert.cert.pem()).unwrap();
        std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
        let port = free_udp_port();
        let config = serde_json::json!({
            "inbounds":[{"tag":"hy", "listen":{"address":"127.0.0.1","port":port},
                "protocol":{"type":"hysteria2","password":"test-password","cert_path":cert_path,"key_path":key_path}}],
            "outbounds":[], "route":{"rules":[],"final":{"type":"direct"}}
        });
        let proxy = spawn_engine(Proxy::new(RuntimeConfig::parse(&config.to_string()).unwrap()).unwrap());
        let mut proxies = vec![proxy];
        let port = if via_hy2 {
            let relay_port = free_udp_port();
            let config = serde_json::json!({
                "inbounds":[{"tag":"hy-in", "listen":{"address":"127.0.0.1","port":relay_port},
                    "protocol":{"type":"hysteria2","password":"test-password","cert_path":cert_path,"key_path":key_path}}],
                "outbounds":[{"tag":"hy-out", "protocol":{"type":"hysteria2","server":"127.0.0.1","port":port,"password":"test-password","insecure":true}}],
                "route":{"rules":[],"final":{"type":"route","outbound":"hy-out"}}
            });
            proxies.push(spawn_engine(Proxy::new(RuntimeConfig::parse(&config.to_string()).unwrap()).unwrap()));
            relay_port
        } else { port };
        tokio::time::sleep(Duration::from_millis(100)).await;
        let echo = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let echo_port = echo.local_addr().unwrap().port();
        let echo_task = tokio::spawn(async move {
            let mut previous_peers = None;
            for _ in 0..2 {
                let mut packets = Vec::new();
                for _ in 0..2 {
                    let mut bytes = vec![0; 4096];
                    let (len, peer) = timeout(Duration::from_secs(5), echo.recv_from(&mut bytes))
                        .await.expect("target receives both isolated requests").unwrap();
                    bytes.truncate(len);
                    packets.push((bytes, peer));
                }
                packets.sort_by_key(|(bytes, _)| bytes[0]);
                let peers = [packets[0].1, packets[1].1];
                assert_ne!(peers[0], peers[1], "same-target sessions need separate direct sockets");
                if let Some(previous) = previous_peers { assert_eq!(peers, previous); }
                previous_peers = Some(peers);
                // Both flows exist before either response is sent.
                for (bytes, peer) in packets.into_iter().rev() {
                    echo.send_to(&bytes, peer).await.unwrap();
                }
            }
        });
        let socks=free_port();
        let client=serde_json::json!({"inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":socks},"protocol":{"type":"socks5"}}],
            "outbounds":[{"tag":"hy","protocol":{"type":"hysteria2","server":"127.0.0.1","port":port,"password":"test-password","insecure":true}}],
            "route":{"final":{"type":"route","outbound":"hy"}}});
        proxies.push(spawn_engine(Proxy::new(RuntimeConfig::parse(&client.to_string()).unwrap()).unwrap()));
        wait_for_listener(socks).await;
        let a_payloads=[vec![1;1600],vec![3;1600]];
        let b_payloads=[vec![2;1600],vec![4;1600]];
        let a_refs=a_payloads.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let b_refs=b_payloads.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let (a_reply,b_reply)=tokio::join!(
            timeout(Duration::from_secs(16),socks5_udp_echo_sequence(socks,echo_port,&a_refs)),
            timeout(Duration::from_secs(16),socks5_udp_echo_sequence(socks,echo_port,&b_refs))
        );
        assert_eq!(a_reply.unwrap(),a_payloads);assert_eq!(b_reply.unwrap(),b_payloads);
        echo_task.await.unwrap();
        for proxy in proxies { proxy.shutdown().await.unwrap(); }
    }).await.unwrap();
}
