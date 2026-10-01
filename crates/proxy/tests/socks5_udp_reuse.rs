#![cfg(all(feature = "socks5", feature = "socks5"))]

use crate::support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::select;
use tokio::sync::watch;
use tokio::time::{sleep, timeout, Duration};
use zero_config::RuntimeConfig;
use zero_core::Address;
use zero_proxy::Proxy as Engine;

use support::{build_udp_packet, free_port, parse_udp_packet, spawn_engine, wait_for_listener};

#[tokio::test]
async fn reuses_upstream_udp_association_within_single_client_session() {
    let upstream_port = free_port();
    let outer_port = free_port();
    let association_count = Arc::new(AtomicUsize::new(0));
    let (stop_tx, mut stop_rx) = watch::channel(false);

    let udp_relay = UdpSocket::bind(("127.0.0.1", 0))
        .await
        .expect("bind upstream udp relay");
    let udp_relay_port = udp_relay.local_addr().expect("relay local addr").port();

    let tcp_counter = Arc::clone(&association_count);
    let tcp_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", upstream_port))
            .await
            .expect("bind upstream tcp listener");

        loop {
            let (mut stream, _) = select! {
                changed = stop_rx.changed() => {
                    changed.expect("watch upstream stop channel");
                    break;
                }
                accepted = listener.accept() => {
                    accepted.expect("accept upstream tcp")
                }
            };

            tcp_counter.fetch_add(1, Ordering::SeqCst);
            stream
                .read_exact(&mut [0_u8; 3])
                .await
                .expect("read auth request");
            stream
                .write_all(&[0x05, 0x00])
                .await
                .expect("write auth response");
            stream
                .read_exact(&mut [0_u8; 10])
                .await
                .expect("read udp associate request");
            stream
                .write_all(&[
                    0x05,
                    0x00,
                    0x00,
                    0x01,
                    127,
                    0,
                    0,
                    1,
                    (udp_relay_port >> 8) as u8,
                    (udp_relay_port & 0xff) as u8,
                ])
                .await
                .expect("write udp associate response");

            tokio::spawn(async move {
                let mut probe = [0_u8; 1];
                loop {
                    match stream.read(&mut probe).await {
                        Ok(0) => break,
                        Ok(_) => continue,
                        Err(_) => break,
                    }
                }
            });
        }
    });

    let udp_task = tokio::spawn(async move {
        let mut buf = [0_u8; 1024];
        for _ in 0..2 {
            let (read, peer) = udp_relay
                .recv_from(&mut buf)
                .await
                .expect("recv upstream udp");
            udp_relay
                .send_to(&buf[..read], peer)
                .await
                .expect("echo upstream udp");
        }
    });

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
                    "tag": "chain",
                    "protocol": {{
                        "type": "socks5",
                        "server": "127.0.0.1",
                        "port": {upstream_port}
                    }}
                }}
            ],
            "route": {{
                "rules": [],
                "final": {{ "type": "route", "outbound": "chain" }}
            }}
        }}"#
    ))
    .expect("parse outer config");
    let outer_engine = Engine::new(outer_config).expect("build outer engine");
    let outer_probe = outer_engine.clone();
    let outer_handle = spawn_engine(outer_engine);

    wait_for_listener(outer_port).await;

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

    for payload in [b"mesh-a".as_slice(), b"mesh-b".as_slice()] {
        let packet = build_udp_packet(&Address::Ipv4([127, 0, 0, 1]), 9000, payload)
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
        assert_eq!(response.payload, payload);
    }

    sleep(Duration::from_millis(100)).await;
    assert_eq!(association_count.load(Ordering::SeqCst), 1);
    let live_stats = outer_probe.stats_snapshot();
    assert_eq!(live_stats.udp_upstream.active_associations, 1);
    assert_eq!(live_stats.udp_upstream.created_associations, 1);
    assert_eq!(live_stats.udp_upstream.reused_associations, 1);
    assert_eq!(live_stats.udp_upstream.closed_associations, 0);
    assert_eq!(live_stats.udp_upstream.idle_timeouts, 0);
    assert_eq!(live_stats.udp_upstream.dropped_associations, 0);
    assert_eq!(live_stats.udp_upstream.failed_association_attempts, 0);
    assert_eq!(live_stats.udp_upstream.send_failures, 0);
    assert_eq!(live_stats.udp_upstream.recv_failures, 0);
    assert_eq!(live_stats.udp_upstream.packets_sent, 2);
    assert_eq!(live_stats.udp_upstream.packets_received, 2);

    drop(control);
    let _ = stop_tx.send(true);
    outer_handle
        .shutdown()
        .await
        .expect("shutdown outer engine");
    let _ = udp_task.await;
    let _ = tcp_task.await;

    let final_stats = outer_probe.stats_snapshot();
    assert_eq!(final_stats.udp_upstream.active_associations, 0);
    assert_eq!(final_stats.udp_upstream.closed_associations, 1);
}
