use super::*;

const USERNAME: &str = "alice";
const PASSWORD: &str = "secret";

#[tokio::test]
#[cfg(all(feature = "socks5", feature = "mieru"))]
async fn relays_udp_through_socks5_to_mieru_relay_chain() {
    let echo_port = free_udp_port();
    let first_hop_port = free_port();
    let final_hop_port = free_udp_port();
    let outer_port = free_port();

    let echo_task = tokio::spawn(async move {
        let socket = UdpSocket::bind(("127.0.0.1", echo_port))
            .await
            .expect("bind udp echo");
        let mut buf = [0_u8; 1024];
        for _ in 0..4 {
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
                    "tag": "first-socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {first_hop_port} }},
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
    .expect("parse first hop config");
    let first_hop_engine = Engine::new(first_hop_config).expect("build first hop engine");
    let first_hop_probe = first_hop_engine.clone();
    let first_hop_handle = spawn_engine(first_hop_engine);

    wait_for_listener(first_hop_port).await;

    let final_hop_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "final-mieru-in",
                    "listen": {{ "address": "127.0.0.1", "port": {final_hop_port} }},
                    "protocol": {{
                        "type": "mieru",
                        "transport": "udp",
                        "users": [
                            {{ "username": "{USERNAME}", "password": "{PASSWORD}" }}
                        ]
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
                    "tag": "first-socks",
                    "protocol": {{
                        "type": "socks5",
                        "server": "127.0.0.1",
                        "port": {first_hop_port}
                    }}
                }},
                {{
                    "tag": "final-mieru",
                    "protocol": {{
                        "type": "mieru",
                        "transport": "udp",
                        "server": "127.0.0.1",
                        "port": {final_hop_port},
                        "username": "{USERNAME}",
                        "password": "{PASSWORD}"
                    }}
                }}
            ],
            "outbound_groups": [
                {{
                    "tag": "udp-relay-chain",
                    "type": "relay",
                    "proxies": ["first-socks", "final-mieru"]
                }}
            ],
            "route": {{
                "rules": [],
                "final": {{ "type": "route", "outbound": "udp-relay-chain" }}
            }}
        }}"#
    ))
    .expect("parse outer config");
    let outer_engine = Engine::new(outer_config).expect("build outer engine");
    let outer_probe = outer_engine.clone();
    let outer_handle = spawn_engine(outer_engine);

    wait_for_listener(outer_port).await;

    let (control, client, relay_port) = open_udp_association(outer_port).await;
    let packet = build_udp_packet(&Address::Ipv4([127, 0, 0, 1]), echo_port, b"mr1")
        .expect("build udp packet");
    client
        .send_to(&packet, ("127.0.0.1", relay_port))
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
    assert_eq!(response.payload, b"mr1");

    let packet = build_udp_packet(&Address::Ipv4([127, 0, 0, 1]), echo_port, b"mr2")
        .expect("build second udp packet");
    client
        .send_to(&packet, ("127.0.0.1", relay_port))
        .await
        .expect("send second udp packet");

    let (read, _) = timeout(Duration::from_secs(3), client.recv_from(&mut buf))
        .await
        .expect("second udp recv timeout")
        .expect("recv second udp response");
    let response = parse_udp_packet(&buf[..read]).expect("parse second udp response");

    assert_eq!(response.target, Address::Ipv4([127, 0, 0, 1]));
    assert_eq!(response.port, echo_port);
    assert_eq!(response.payload, b"mr2");

    let (control2, client2, relay_port2) = open_udp_association(outer_port).await;
    let left = build_udp_packet(&Address::Ipv4([127, 0, 0, 1]), echo_port, b"left")
        .expect("build left packet");
    let right = build_udp_packet(&Address::Ipv4([127, 0, 0, 1]), echo_port, b"rght")
        .expect("build right packet");
    client
        .send_to(&left, ("127.0.0.1", relay_port))
        .await
        .expect("send left packet");
    client2
        .send_to(&right, ("127.0.0.1", relay_port2))
        .await
        .expect("send right packet");

    let (read, _) = timeout(Duration::from_secs(3), client.recv_from(&mut buf))
        .await
        .expect("left udp recv timeout")
        .expect("recv left response");
    assert_eq!(
        parse_udp_packet(&buf[..read])
            .expect("parse left response")
            .payload,
        b"left"
    );
    let (read, _) = timeout(Duration::from_secs(3), client2.recv_from(&mut buf))
        .await
        .expect("right udp recv timeout")
        .expect("recv right response");
    assert_eq!(
        parse_udp_packet(&buf[..read])
            .expect("parse right response")
            .payload,
        b"rght"
    );

    wait_for(
        "two UDP sessions to share one Mieru datagram relay carrier",
        || {
            first_hop_probe
                .active_sessions()
                .iter()
                .filter(|session| {
                    session.network == zero_core::Network::Udp && session.port == final_hop_port
                })
                .count()
                == 1
        },
    )
    .await;

    wait_for(
        "outer udp session to record mieru relay chain outbound",
        || {
            outer_probe
                .active_sessions()
                .first()
                .map(|session| {
                    session.network == zero_core::Network::Udp
                        && session.outbound_tag.as_deref() == Some("final-mieru")
                        && session.protocol == zero_core::ProtocolType::new("socks5")
                        && session.bytes_up > 0
                        && session.bytes_down > 0
                })
                .unwrap_or(false)
        },
    )
    .await;

    drop(control);
    drop(control2);
    wait_for("outer udp mieru relay chain session to complete", || {
        outer_probe
            .completed_sessions()
            .first()
            .map(|session| {
                session.network == zero_core::Network::Udp
                    && session.outbound_tag.as_deref() == Some("final-mieru")
                    && session.outcome.kind() == "chained_relayed"
                    && session.bytes_up > 0
                    && session.bytes_down > 0
            })
            .unwrap_or(false)
    })
    .await;

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

#[tokio::test]
#[cfg(all(feature = "socks5", feature = "mieru"))]
async fn relays_udp_through_socks5_to_mieru_tcp_relay_chain_with_pooling() {
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
            "inbounds": [{{
                "tag": "first-socks-in",
                "listen": {{ "address": "127.0.0.1", "port": {first_hop_port} }},
                "protocol": {{ "type": "socks5" }}
            }}],
            "outbounds": [],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#
    ))
    .expect("parse first hop config");
    let first_hop_engine = Engine::new(first_hop_config).expect("build first hop engine");
    let first_hop_probe = first_hop_engine.clone();
    let first_hop_handle = spawn_engine(first_hop_engine);
    wait_for_listener(first_hop_port).await;

    let final_hop_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [{{
                "tag": "final-mieru-in",
                "listen": {{ "address": "127.0.0.1", "port": {final_hop_port} }},
                "protocol": {{
                    "type": "mieru",
                    "users": [{{ "username": "{USERNAME}", "password": "{PASSWORD}" }}]
                }}
            }}],
            "outbounds": [],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#
    ))
    .expect("parse final hop config");
    let final_hop_engine = Engine::new(final_hop_config).expect("build final hop engine");
    let final_hop_handle = spawn_engine(final_hop_engine);
    wait_for_listener(final_hop_port).await;

    let outer_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [{{
                "tag": "outer-socks-in",
                "listen": {{ "address": "127.0.0.1", "port": {outer_port} }},
                "protocol": {{ "type": "socks5" }}
            }}],
            "outbounds": [
                {{
                    "tag": "first-socks",
                    "protocol": {{
                        "type": "socks5",
                        "server": "127.0.0.1",
                        "port": {first_hop_port}
                    }}
                }},
                {{
                    "tag": "final-mieru",
                    "protocol": {{
                        "type": "mieru",
                        "server": "127.0.0.1",
                        "port": {final_hop_port},
                        "username": "{USERNAME}",
                        "password": "{PASSWORD}"
                    }}
                }}
            ],
            "outbound_groups": [{{
                "tag": "udp-tcp-relay-chain",
                "type": "relay",
                "proxies": ["first-socks", "final-mieru"]
            }}],
            "route": {{
                "rules": [],
                "final": {{ "type": "route", "outbound": "udp-tcp-relay-chain" }}
            }}
        }}"#
    ))
    .expect("parse outer config");
    let outer_engine = Engine::new(outer_config).expect("build outer engine");
    let outer_handle = spawn_engine(outer_engine);
    wait_for_listener(outer_port).await;

    let (first_control, first_client, first_relay_port) = open_udp_association(outer_port).await;
    let (second_control, second_client, second_relay_port) = open_udp_association(outer_port).await;
    let first_packet = build_udp_packet(&Address::Ipv4([127, 0, 0, 1]), echo_port, b"one1")
        .expect("build first packet");
    let second_packet = build_udp_packet(&Address::Ipv4([127, 0, 0, 1]), echo_port, b"two2")
        .expect("build second packet");
    first_client
        .send_to(&first_packet, ("127.0.0.1", first_relay_port))
        .await
        .expect("send first packet");
    second_client
        .send_to(&second_packet, ("127.0.0.1", second_relay_port))
        .await
        .expect("send second packet");

    let mut first_buf = [0_u8; 1024];
    let (first_read, _) = timeout(
        Duration::from_secs(3),
        first_client.recv_from(&mut first_buf),
    )
    .await
    .expect("first udp recv timeout")
    .expect("recv first udp response");
    assert_eq!(
        parse_udp_packet(&first_buf[..first_read])
            .expect("parse first response")
            .payload,
        b"one1"
    );

    let mut second_buf = [0_u8; 1024];
    let (second_read, _) = timeout(
        Duration::from_secs(3),
        second_client.recv_from(&mut second_buf),
    )
    .await
    .expect("second udp recv timeout")
    .expect("recv second udp response");
    assert_eq!(
        parse_udp_packet(&second_buf[..second_read])
            .expect("parse second response")
            .payload,
        b"two2"
    );

    wait_for(
        "two UDP associations to share one Mieru TCP relay carrier",
        || {
            first_hop_probe
                .active_sessions()
                .iter()
                .filter(|session| {
                    session.network == zero_core::Network::Tcp && session.port == final_hop_port
                })
                .count()
                == 1
        },
    )
    .await;

    drop(first_control);
    drop(second_control);
    echo_task.await.expect("join udp echo task");
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
}

async fn open_udp_association(outer_port: u16) -> (TcpStream, UdpSocket, u16) {
    let mut control = TcpStream::connect(("127.0.0.1", outer_port))
        .await
        .expect("connect outer proxy");
    control
        .write_all(&[0x05, 0x01, 0x00])
        .await
        .expect("write auth");
    let mut auth = [0_u8; 2];
    control.read_exact(&mut auth).await.expect("read auth");
    assert_eq!(auth, [0x05, 0x00]);
    control
        .write_all(&[
            0x05, 0x03, 0x00, 0x01, // udp associate + ipv4
            0, 0, 0, 0, 0x00, 0x00,
        ])
        .await
        .expect("write udp associate");
    let mut response = [0_u8; 10];
    control
        .read_exact(&mut response)
        .await
        .expect("read udp associate response");
    assert_eq!(response[1], 0x00);
    let relay_port = u16::from_be_bytes([response[8], response[9]]);
    let client = UdpSocket::bind(("127.0.0.1", 0))
        .await
        .expect("bind udp client");
    (control, client, relay_port)
}
