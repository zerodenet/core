#![cfg(feature = "socks5")]

mod support;

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::oneshot;
use tokio::time::timeout;
use zero_config::RuntimeConfig;
use zero_core::Address;
use zero_proxy::Proxy as Engine;

use support::{
    build_udp_packet, free_port, free_udp_port, parse_udp_packet, spawn_engine, wait_for,
    wait_for_listener,
};

#[tokio::test]
async fn tracks_live_bytes_and_completed_session_history() {
    let echo_port = free_port();
    let proxy_port = free_port();
    let (payload_read_tx, payload_read_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();

    let echo_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", echo_port))
            .await
            .expect("bind echo");
        let (mut stream, _) = listener.accept().await.expect("accept echo");
        let mut buf = [0_u8; 4];
        stream.read_exact(&mut buf).await.expect("read echo");
        let _ = payload_read_tx.send(());
        let _ = release_rx.await;
        stream.write_all(&buf).await.expect("write echo");
        stream.read_exact(&mut buf).await.expect("read second echo");
        stream.write_all(&buf).await.expect("write second echo");
    });

    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {proxy_port} }},
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
    .expect("parse config");
    let engine = Engine::new(config).expect("build engine");
    let probe = engine.clone();
    let handle = spawn_engine(engine);

    wait_for_listener(proxy_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", proxy_port))
        .await
        .expect("connect proxy");
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

    client.write_all(b"ping").await.expect("write payload");
    let _ = payload_read_rx.await;

    // Session byte tracking counts only relay-phase bytes (not SOCKS5
    // handshake overhead).  At this point "ping" has been relayed to
    // the echo server but the response is blocked by release_rx.
    //   max(in_rx=4, out_tx=4) = bytes_up=4
    //   max(in_tx=0, out_rx=0) = bytes_down=0
    wait_for("active session to record upload bytes", || {
        probe
            .active_sessions()
            .first()
            .map(|session| session.bytes_up == 4 && session.bytes_down == 0)
            .unwrap_or(false)
    })
    .await;

    let active = probe.active_sessions();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].network, zero_core::Network::Tcp);
    assert_eq!(active[0].mode, "rule");
    assert_eq!(active[0].inbound_tag.as_deref(), Some("socks-in"));
    assert_eq!(active[0].outbound_tag.as_deref(), Some("direct"));
    assert_eq!(active[0].bytes_up, 4);
    assert_eq!(active[0].bytes_down, 0);
    assert_eq!(active[0].inbound_rx_bytes, 4);
    assert_eq!(active[0].inbound_tx_bytes, 0);
    assert_eq!(active[0].outbound_tx_bytes, 4);

    let _ = release_tx.send(());
    let mut echoed = [0_u8; 4];
    client.read_exact(&mut echoed).await.expect("read payload");
    assert_eq!(&echoed, b"ping");

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    client
        .write_all(b"pong")
        .await
        .expect("write second payload");
    client
        .read_exact(&mut echoed)
        .await
        .expect("read second payload");
    assert_eq!(&echoed, b"pong");

    wait_for("active session to expose sampled throughput", || {
        probe
            .active_sessions()
            .first()
            .map(|session| session.throughput_up_bps > 0 && session.throughput_down_bps > 0)
            .unwrap_or(false)
    })
    .await;

    // After "ping" echo + "pong" exchange: 8 bytes each way through relay.
    //   max(in_rx=8, out_tx=8) = bytes_up=8
    //   max(in_tx=8, out_rx=8) = bytes_down=8
    let active = probe.active_sessions();
    assert_eq!(active[0].bytes_up, 8);
    assert_eq!(active[0].bytes_down, 8);
    assert!(active[0].throughput_up_bps > 0);
    assert!(active[0].throughput_down_bps > 0);
    drop(client);

    wait_for("completed session history to be visible", || {
        let completed = probe.completed_sessions();
        completed
            .first()
            .map(|session| session.bytes_down == 8)
            .unwrap_or(false)
    })
    .await;

    handle.shutdown().await.expect("shutdown engine");
    let _ = echo_task.await;

    let completed = probe.completed_sessions();
    assert!(!completed.is_empty());
    assert_eq!(completed[0].network, zero_core::Network::Tcp);
    assert_eq!(completed[0].mode, "rule");
    assert_eq!(completed[0].outcome.kind(), "direct_relayed");
    assert_eq!(completed[0].bytes_up, 8);
    assert_eq!(completed[0].bytes_down, 8);
    assert_eq!(completed[0].inbound_rx_bytes, 8);
    assert_eq!(completed[0].inbound_tx_bytes, 8);
    assert_eq!(completed[0].outbound_rx_bytes, 8);
    assert_eq!(completed[0].outbound_tx_bytes, 8);
    assert!(probe.active_sessions().is_empty());
}

#[tokio::test]
async fn tracks_udp_flow_with_tcp_session_observability_shape() {
    let echo_port = free_udp_port();
    let proxy_port = free_port();
    let (payload_read_tx, payload_read_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();

    let echo_task = tokio::spawn(async move {
        let socket = UdpSocket::bind(("127.0.0.1", echo_port))
            .await
            .expect("bind udp echo");
        let mut buf = [0_u8; 1024];
        let (read, peer) = socket.recv_from(&mut buf).await.expect("recv udp");
        let _ = payload_read_tx.send(());
        let _ = release_rx.await;
        socket
            .send_to(&buf[..read], peer)
            .await
            .expect("send udp echo");
    });

    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {proxy_port} }},
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
    .expect("parse config");
    let engine = Engine::new(config).expect("build engine");
    let probe = engine.clone();
    let handle = spawn_engine(engine);

    wait_for_listener(proxy_port).await;

    let mut control = TcpStream::connect(("127.0.0.1", proxy_port))
        .await
        .expect("connect proxy");
    control
        .write_all(&[0x05, 0x01, 0x00])
        .await
        .expect("write auth");

    let mut auth = [0_u8; 2];
    control.read_exact(&mut auth).await.expect("read auth");
    assert_eq!(auth, [0x05, 0x00]);

    control
        .write_all(&[
            0x05, 0x03, 0x00, 0x01, //
            0, 0, 0, 0, 0x00, 0x00,
        ])
        .await
        .expect("write udp associate");

    let mut associate_response = [0_u8; 10];
    control
        .read_exact(&mut associate_response)
        .await
        .expect("read udp associate response");
    assert_eq!(associate_response[1], 0x00);
    let relay_port = u16::from_be_bytes([associate_response[8], associate_response[9]]);

    let client = UdpSocket::bind(("127.0.0.1", 0))
        .await
        .expect("bind udp client");
    let packet = build_udp_packet(&Address::Ipv4([127, 0, 0, 1]), echo_port, b"ping")
        .expect("build udp packet");
    client
        .send_to(&packet, ("127.0.0.1", relay_port))
        .await
        .expect("send udp packet");

    let _ = payload_read_rx.await;

    wait_for("udp active session to record upload bytes", || {
        probe
            .active_sessions()
            .first()
            .map(|session| {
                session.network == zero_core::Network::Udp
                    && session.bytes_up == 27
                    && session.bytes_down == 12
            })
            .unwrap_or(false)
    })
    .await;

    let active = probe.active_sessions();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].network, zero_core::Network::Udp);
    assert_eq!(active[0].protocol, zero_core::ProtocolType::new("socks5"));
    assert_eq!(active[0].mode, "rule");
    assert_eq!(active[0].inbound_tag.as_deref(), Some("socks-in"));
    assert_eq!(active[0].outbound_tag.as_deref(), Some("direct"));
    assert_eq!(active[0].bytes_up, 27);
    assert_eq!(active[0].bytes_down, 12);
    assert_eq!(active[0].inbound_rx_bytes, 27);
    assert_eq!(active[0].inbound_tx_bytes, 12);
    assert_eq!(active[0].outbound_tx_bytes, 4);
    assert_eq!(probe.stats_snapshot().active_sessions, 1);

    let _ = release_tx.send(());
    let mut buf = [0_u8; 1024];
    let (read, _) = timeout(Duration::from_secs(3), client.recv_from(&mut buf))
        .await
        .expect("udp recv timeout")
        .expect("recv udp response");
    let response = parse_udp_packet(&buf[..read]).expect("parse udp response");
    assert_eq!(response.payload, b"ping");

    wait_for("udp active session to record download bytes", || {
        probe
            .active_sessions()
            .first()
            .map(|session| session.bytes_up == 27 && session.bytes_down == 26)
            .unwrap_or(false)
    })
    .await;

    let active = probe.active_sessions();
    assert_eq!(active[0].bytes_up, 27);
    assert_eq!(active[0].bytes_down, 26);
    assert_eq!(active[0].inbound_tx_bytes, 26);
    assert_eq!(active[0].outbound_rx_bytes, 4);

    drop(client);
    drop(control);

    wait_for("udp completed session history to be visible", || {
        let completed = probe.completed_sessions();
        completed
            .first()
            .map(|session| {
                session.network == zero_core::Network::Udp
                    && session.bytes_up == 27
                    && session.bytes_down == 26
            })
            .unwrap_or(false)
    })
    .await;

    handle.shutdown().await.expect("shutdown engine");
    let _ = echo_task.await;

    let completed = probe.completed_sessions();
    assert!(!completed.is_empty());
    assert_eq!(completed[0].network, zero_core::Network::Udp);
    assert_eq!(
        completed[0].protocol,
        zero_core::ProtocolType::new("socks5")
    );
    assert_eq!(completed[0].mode, "rule");
    assert_eq!(completed[0].outbound_tag.as_deref(), Some("direct"));
    assert_eq!(completed[0].outcome.kind(), "direct_relayed");
    assert_eq!(completed[0].bytes_up, 27);
    assert_eq!(completed[0].bytes_down, 26);
    assert_eq!(completed[0].inbound_rx_bytes, 27);
    assert_eq!(completed[0].inbound_tx_bytes, 26);
    assert_eq!(completed[0].outbound_rx_bytes, 4);
    assert_eq!(completed[0].outbound_tx_bytes, 4);
    assert!(probe.active_sessions().is_empty());

    let stats = probe.stats_snapshot();
    assert_eq!(stats.total_started, 1);
    assert_eq!(stats.active_sessions, 0);
    assert_eq!(stats.completed_sessions, 1);
    assert_eq!(stats.direct_sessions, 1);
}
