use super::*;

const PASSWORD: &str = "test-password";
const CIPHER: &str = "aes-128-gcm";

#[tokio::test]
#[cfg(all(feature = "socks5", feature = "shadowsocks"))]
async fn relays_tcp_through_socks5_to_shadowsocks_to_socks5_relay_chain() {
    let echo_port = free_port();
    let first_hop_port = free_port();
    let final_hop_port = free_port();
    let outer_port = free_port();

    let echo_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", echo_port))
            .await
            .expect("bind echo");
        let (mut stream, _) = listener.accept().await.expect("accept echo");
        let mut buf = [0_u8; 4];
        stream.read_exact(&mut buf).await.expect("read echo");
        stream.write_all(&buf).await.expect("write echo");
    });

    let first_hop_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "first-ss-in",
                    "listen": {{ "address": "127.0.0.1", "port": {first_hop_port} }},
                    "protocol": {{
                        "type": "shadowsocks",
                        "password": "{PASSWORD}",
                        "cipher": "{CIPHER}"
                    }}
                }}
            ],
            "outbounds": [],
            "route": {{
                "rules": [],
                "final": {{ "type": "direct" }}
            }}
        }}"#
    ))
    .expect("parse first hop config");
    let first_hop_engine = Engine::new(first_hop_config).expect("build first hop engine");
    let first_hop_handle = spawn_engine(first_hop_engine);

    wait_for_listener(first_hop_port).await;

    let final_hop_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "final-socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {final_hop_port} }},
                    "protocol": {{ "type": "socks5" }}
                }}
            ],
            "outbounds": [],
            "route": {{
                "rules": [],
                "final": {{ "type": "direct" }}
            }}
        }}"#
    ))
    .expect("parse final hop config");
    let final_hop_engine = Engine::new(final_hop_config).expect("build final hop engine");
    let final_hop_handle = spawn_engine(final_hop_engine);

    wait_for_listener(final_hop_port).await;

    let outer_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "outer-socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {outer_port} }},
                    "protocol": {{ "type": "socks5" }}
                }}
            ],
            "outbounds": [
                {{
                    "tag": "first-ss",
                    "protocol": {{
                        "type": "shadowsocks",
                        "server": "127.0.0.1",
                        "port": {first_hop_port},
                        "password": "{PASSWORD}",
                        "cipher": "{CIPHER}"
                    }}
                }},
                {{
                    "tag": "final-socks",
                    "protocol": {{
                        "type": "socks5",
                        "server": "127.0.0.1",
                        "port": {final_hop_port}
                    }}
                }}
            ],
            "outbound_groups": [
                {{
                    "tag": "tcp-relay-chain",
                    "type": "relay",
                    "proxies": ["first-ss", "final-socks"]
                }}
            ],
            "route": {{
                "rules": [],
                "final": {{ "type": "route", "outbound": "tcp-relay-chain" }}
            }}
        }}"#
    ))
    .expect("parse outer config");
    let outer_engine = Engine::new(outer_config).expect("build outer engine");
    let outer_probe = outer_engine.clone();
    let outer_handle = spawn_engine(outer_engine);

    wait_for_listener(outer_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", outer_port))
        .await
        .expect("connect outer proxy");
    client
        .write_all(&[0x05, 0x01, 0x00])
        .await
        .expect("write auth");

    let mut auth = [0_u8; 2];
    client.read_exact(&mut auth).await.expect("read auth");
    assert_eq!(auth, [0x05, 0x00]);

    let request = [
        0x05,
        0x01,
        0x00,
        0x01,
        127,
        0,
        0,
        1,
        ((echo_port >> 8) & 0xff) as u8,
        (echo_port & 0xff) as u8,
    ];
    client.write_all(&request).await.expect("write request");

    let mut response = [0_u8; 10];
    client
        .read_exact(&mut response)
        .await
        .expect("read response");
    assert_eq!(response[1], 0x00);

    client.write_all(b"sshp").await.expect("write payload");
    let mut echoed = [0_u8; 4];
    client.read_exact(&mut echoed).await.expect("read payload");
    assert_eq!(&echoed, b"sshp");

    let statistics = |tag: &str| {
        outer_probe
            .engine()
            .traffic_snapshot(&zero_api::TrafficGetQuery {
                scope: zero_api::TrafficScope::Outbound { tag: tag.into() },
            })
            .unwrap()
    };
    let first = statistics("first-ss");
    let last = statistics("final-socks");
    // The final carrier includes SOCKS handshake; the first also includes
    // Shadowsocks salt/tags/framing. They are distinct from final Flow bytes.
    assert!(last.planes[2].counters.tx_bytes.unwrap() > 4);
    assert!(first.planes[2].counters.tx_bytes.unwrap() > last.planes[2].counters.tx_bytes.unwrap());
    assert!(first.planes[2].counters.rx_bytes.unwrap() > last.planes[2].counters.rx_bytes.unwrap());
    assert_eq!(first.planes[2].counters.tx_packets, None);
    assert_eq!(last.planes[2].counters.rx_packets, None);
    assert_eq!(first.planes[0].counters.tx_bytes, Some(0));
    assert!(last.planes[0].counters.tx_bytes.unwrap() >= 4);

    outer_handle
        .shutdown()
        .await
        .expect("shutdown outer engine");
    final_hop_handle
        .shutdown()
        .await
        .expect("shutdown final hop engine");
    first_hop_handle
        .shutdown()
        .await
        .expect("shutdown first hop engine");
    let _ = echo_task.await;
}
