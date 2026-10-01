use crate::support;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use zero_config::RuntimeConfig;
use zero_proxy::Proxy as Engine;

use support::{free_port, spawn_engine, wait_for, wait_for_listener};

#[tokio::test]
async fn tracks_direct_and_blocked_sessions_in_stats() {
    let echo_port = free_port();
    let direct_port = free_port();
    let blocked_port = free_port();

    let echo_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", echo_port))
            .await
            .expect("bind echo");
        let (mut stream, _) = listener.accept().await.expect("accept echo");
        let mut buf = [0_u8; 4];
        stream.read_exact(&mut buf).await.expect("read echo");
        stream.write_all(&buf).await.expect("write echo");
    });

    let direct_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "socks-direct",
                    "listen": {{ "address": "127.0.0.1", "port": {direct_port} }},
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
    .expect("parse direct config");
    let direct_engine = Engine::new(direct_config).expect("build direct engine");
    let direct_probe = direct_engine.clone();
    let direct_handle = spawn_engine(direct_engine);

    wait_for_listener(direct_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", direct_port))
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
    let mut echoed = [0_u8; 4];
    client.read_exact(&mut echoed).await.expect("read payload");
    assert_eq!(&echoed, b"ping");
    drop(client);

    wait_for("direct session to drain", || {
        direct_probe.stats_snapshot().active_sessions == 0
    })
    .await;

    direct_handle
        .shutdown()
        .await
        .expect("shutdown direct engine");
    let _ = echo_task.await;

    let direct_stats = direct_probe.stats_snapshot();
    assert_eq!(direct_stats.total_started, 1);
    assert_eq!(direct_stats.active_sessions, 0);
    assert_eq!(direct_stats.completed_sessions, 1);
    assert_eq!(direct_stats.failed_sessions, 0);
    assert_eq!(direct_stats.blocked_sessions, 0);
    assert_eq!(direct_stats.direct_sessions, 1);
    assert_eq!(direct_stats.chained_sessions, 0);
    assert!(direct_probe.active_sessions().is_empty());

    let blocked_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "socks-blocked",
                    "listen": {{ "address": "127.0.0.1", "port": {blocked_port} }},
                    "protocol": {{ "type": "socks5" }}
                }}
            ],
            "outbounds": [],
            "route": {{
                "rules": [
                    {{
                        "condition": {{
                            "type": "domain",
                            "values": ["blocked.example"]
                        }},
                        "action": {{ "type": "reject" }}
                    }}
                ],
                "final": {{ "type": "direct" }}
            }}
        }}"#
    ))
    .expect("parse blocked config");
    let blocked_engine = Engine::new(blocked_config).expect("build blocked engine");
    let blocked_probe = blocked_engine.clone();
    let blocked_handle = spawn_engine(blocked_engine);

    wait_for_listener(blocked_port).await;

    let mut blocked_client = TcpStream::connect(("127.0.0.1", blocked_port))
        .await
        .expect("connect blocked proxy");
    blocked_client
        .write_all(&[0x05, 0x01, 0x00])
        .await
        .expect("write auth");
    blocked_client
        .read_exact(&mut auth)
        .await
        .expect("read auth");

    let mut blocked_request = vec![0x05, 0x01, 0x00, 0x03, 0x0f];
    blocked_request.extend_from_slice(b"blocked.example");
    blocked_request.extend_from_slice(&443_u16.to_be_bytes());
    blocked_client
        .write_all(&blocked_request)
        .await
        .expect("write request");
    blocked_client
        .read_exact(&mut response)
        .await
        .expect("read response");
    assert_eq!(response[1], 0x02);
    drop(blocked_client);

    wait_for("blocked session to drain", || {
        blocked_probe.stats_snapshot().active_sessions == 0
    })
    .await;

    blocked_handle
        .shutdown()
        .await
        .expect("shutdown blocked engine");

    let blocked_stats = blocked_probe.stats_snapshot();
    assert_eq!(blocked_stats.total_started, 1);
    assert_eq!(blocked_stats.active_sessions, 0);
    assert_eq!(blocked_stats.completed_sessions, 0);
    assert_eq!(blocked_stats.failed_sessions, 0);
    assert_eq!(blocked_stats.blocked_sessions, 1);
    assert_eq!(blocked_stats.direct_sessions, 0);
    assert_eq!(blocked_stats.chained_sessions, 0);
    assert!(blocked_probe.active_sessions().is_empty());
}

#[tokio::test]
async fn exposes_active_session_snapshot_while_connection_is_open() {
    let echo_port = free_port();
    let proxy_port = free_port();
    let (echo_ready_tx, echo_ready_rx) = oneshot::channel();
    let (release_echo_tx, release_echo_rx) = oneshot::channel();

    let echo_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", echo_port))
            .await
            .expect("bind echo");
        let (mut stream, _) = listener.accept().await.expect("accept echo");
        let _ = echo_ready_tx.send(());
        let _ = release_echo_rx.await;
        let mut buf = [0_u8; 4];
        stream.read_exact(&mut buf).await.expect("read echo");
        stream.write_all(&buf).await.expect("write echo");
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

    let _ = echo_ready_rx.await;

    let active = probe.active_sessions();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].inbound_tag.as_deref(), Some("socks-in"));
    assert_eq!(active[0].outbound_tag.as_deref(), Some("direct"));
    assert_eq!(probe.stats_snapshot().active_sessions, 1);

    let _ = release_echo_tx.send(());
    client.write_all(b"hold").await.expect("write payload");
    let mut echoed = [0_u8; 4];
    client.read_exact(&mut echoed).await.expect("read payload");
    assert_eq!(&echoed, b"hold");
    drop(client);

    wait_for("active session snapshot to clear", || {
        probe.stats_snapshot().active_sessions == 0
    })
    .await;

    handle.shutdown().await.expect("shutdown engine");
    let _ = echo_task.await;

    assert!(probe.active_sessions().is_empty());
    let stats = probe.stats_snapshot();
    assert_eq!(stats.total_started, 1);
    assert_eq!(stats.active_sessions, 0);
    assert_eq!(stats.completed_sessions, 1);
}
