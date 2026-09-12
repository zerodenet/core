use super::relays_udp_through_shadowsocks_outbound_all_ciphers::password_for_cipher;
use super::*;

#[tokio::test]
#[cfg(all(feature = "socks5", feature = "shadowsocks"))]
async fn relays_udp_through_shadowsocks_to_shadowsocks_relay_chain() {
    for cipher in [
        "aes-128-gcm",
        "2022-blake3-aes-128-gcm",
        "2022-blake3-aes-256-gcm",
        "2022-blake3-chacha20-poly1305",
    ] {
        let first_password = password_for_cipher(cipher);
        let final_password = match cipher {
            "2022-blake3-aes-128-gcm" => "ZmVkY2JhOTg3NjU0MzIxMA==",
            "2022-blake3-aes-256-gcm" | "2022-blake3-chacha20-poly1305" => {
                "ZmVkY2JhOTg3NjU0MzIxMGZlZGNiYTk4NzY1NDMyMTA="
            }
            _ => "final-password",
        };
        let echo_port = free_udp_port();
        let first_hop_port = free_port();
        let final_hop_port = free_port();
        let outer_port = free_port();

        let echo_task = tokio::spawn(async move {
            let socket = UdpSocket::bind(("127.0.0.1", echo_port))
                .await
                .expect("bind udp echo");
            let mut buf = [0_u8; 1024];
            for _ in 0..2 {
                let (read, peer) = socket.recv_from(&mut buf).await.expect("recv udp");
                socket
                    .send_to(&buf[..read], peer)
                    .await
                    .expect("send udp echo");
            }
        });

        let first_hop_config = RuntimeConfig::parse(&format!(
            r#"{{
            "inbounds": [
                {{
                    "tag": "first-ss-in",
                    "listen": {{ "address": "127.0.0.1", "port": {first_hop_port} }},
                    "protocol": {{
                        "type": "shadowsocks",
                        "password": "{first_password}",
                        "cipher": "{cipher}"
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
                    "tag": "final-ss-in",
                    "listen": {{ "address": "127.0.0.1", "port": {final_hop_port} }},
                    "protocol": {{
                        "type": "shadowsocks",
                        "password": "{final_password}",
                        "cipher": "{cipher}"
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
                        "password": "{first_password}",
                        "cipher": "{cipher}"
                    }}
                }},
                {{
                    "tag": "final-ss",
                    "protocol": {{
                        "type": "shadowsocks",
                        "server": "127.0.0.1",
                        "port": {final_hop_port},
                        "password": "{final_password}",
                        "cipher": "{cipher}"
                    }}
                }}
            ],
            "outbound_groups": [
                {{
                    "tag": "ss-udp-relay-chain",
                    "type": "relay",
                    "proxies": ["first-ss", "final-ss"]
                }}
            ],
            "route": {{
                "rules": [],
                "final": {{ "type": "route", "outbound": "ss-udp-relay-chain" }}
            }}
        }}"#
        ))
        .expect("parse outer config");
        let outer_engine = Engine::new(outer_config).expect("build outer engine");
        let outer_probe = outer_engine.clone();
        let outer_handle = spawn_engine(outer_engine);

        wait_for_listener(outer_port).await;

        let mut control = timeout(
            Duration::from_secs(3),
            TcpStream::connect(("127.0.0.1", outer_port)),
        )
        .await
        .expect("connect outer proxy timeout")
        .expect("connect outer proxy");
        timeout(
            Duration::from_secs(3),
            control.write_all(&[0x05, 0x01, 0x00]),
        )
        .await
        .expect("write auth timeout")
        .expect("write auth");

        let mut auth = [0_u8; 2];
        timeout(Duration::from_secs(3), control.read_exact(&mut auth))
            .await
            .expect("read auth timeout")
            .expect("read auth");
        assert_eq!(auth, [0x05, 0x00]);

        timeout(
            Duration::from_secs(3),
            control.write_all(&[
                0x05, 0x03, 0x00, 0x01, // udp associate + ipv4
                0, 0, 0, 0, 0x00, 0x00,
            ]),
        )
        .await
        .expect("write udp associate timeout")
        .expect("write udp associate");

        let mut response = [0_u8; 10];
        timeout(Duration::from_secs(3), control.read_exact(&mut response))
            .await
            .expect("read udp associate response timeout")
            .expect("read udp associate response");
        assert_eq!(response[1], 0x00);
        let relay_port = u16::from_be_bytes([response[8], response[9]]);

        let client = UdpSocket::bind(("127.0.0.1", 0))
            .await
            .expect("bind udp client");
        let first_packet = build_udp_packet(&Address::Ipv4([127, 0, 0, 1]), echo_port, b"ss2a")
            .expect("build udp packet");
        client
            .send_to(&first_packet, ("127.0.0.1", relay_port))
            .await
            .expect("send udp packet");

        let mut buf = [0_u8; 1024];
        let (read, _) = timeout(Duration::from_secs(3), client.recv_from(&mut buf))
            .await
            .expect("udp recv timeout")
            .expect("recv udp response");
        let response = parse_udp_packet(&buf[..read]).expect("parse udp response");

        assert_eq!(response.target, Address::Ipv4([127, 0, 0, 1]));
        assert_eq!(response.port, echo_port);
        assert_eq!(response.payload, b"ss2a");

        let second_packet = build_udp_packet(&Address::Ipv4([127, 0, 0, 1]), echo_port, b"ss2b")
            .expect("build second udp packet");
        client
            .send_to(&second_packet, ("127.0.0.1", relay_port))
            .await
            .expect("send second udp packet");

        let (read, _) = timeout(Duration::from_secs(3), client.recv_from(&mut buf))
            .await
            .expect("second udp recv timeout")
            .expect("recv second udp response");
        let response = parse_udp_packet(&buf[..read]).expect("parse second udp response");

        assert_eq!(response.target, Address::Ipv4([127, 0, 0, 1]));
        assert_eq!(response.port, echo_port);
        assert_eq!(response.payload, b"ss2b");

        wait_for(
            "outer udp session to record shadowsocks relay chain outbound",
            || {
                outer_probe
                    .active_sessions()
                    .first()
                    .map(|session| {
                        session.network == zero_core::Network::Udp
                            && session.outbound_tag.as_deref() == Some("final-ss")
                            && session.protocol == zero_core::ProtocolType::new("socks5")
                            && session.bytes_up > 0
                            && session.bytes_down > 0
                    })
                    .unwrap_or(false)
            },
        )
        .await;

        drop(control);
        wait_for(
            "outer udp shadowsocks relay chain session to complete",
            || {
                outer_probe
                    .completed_sessions()
                    .first()
                    .map(|session| {
                        session.network == zero_core::Network::Udp
                            && session.outbound_tag.as_deref() == Some("final-ss")
                            && session.outcome.kind() == "chained_relayed"
                            && session.bytes_up > 0
                            && session.bytes_down > 0
                    })
                    .unwrap_or(false)
            },
        )
        .await;

        timeout(Duration::from_secs(3), outer_handle.shutdown())
            .await
            .expect("shutdown outer engine timeout")
            .expect("shutdown outer engine");
        timeout(Duration::from_secs(3), final_hop_handle.shutdown())
            .await
            .expect("shutdown final hop engine timeout")
            .expect("shutdown final hop engine");
        timeout(Duration::from_secs(3), first_hop_handle.shutdown())
            .await
            .expect("shutdown first hop engine timeout")
            .expect("shutdown first hop engine");
        timeout(Duration::from_secs(3), echo_task)
            .await
            .expect("join echo timeout")
            .expect("join echo task");
    }
}
