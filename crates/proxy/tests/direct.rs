//! Direct inbound integration tests.

use crate::support;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy as Engine;

use support::{free_port, spawn_engine, wait_for_listener};

#[tokio::test]
async fn relays_raw_tcp_to_fixed_ipv4_target() {
    let echo_port = free_port();
    let proxy_port = free_port();

    // Echo server accepts multiple connections so that the `wait_for_listener`
    // TCP probe (treated as real traffic by direct inbound) doesn't consume
    // the sole accept slot.  Each connection gets its own task.
    let echo_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", echo_port))
            .await
            .expect("bind echo");
        loop {
            let (mut stream, _) = listener.accept().await.expect("accept echo");
            tokio::spawn(async move {
                let mut buf = [0_u8; 4];
                if stream.read_exact(&mut buf).await.is_err() {
                    return; // probe or other noise — ignore
                }
                let _ = stream.write_all(&buf).await;
            });
        }
    });

    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "direct-in",
                    "listen": {{ "address": "127.0.0.1", "port": {proxy_port} }},
                    "protocol": {{
                        "type": "direct",
                        "target": "127.0.0.1",
                        "port": {echo_port}
                    }}
                }}
            ],
            "route": {{
                "rules": [],
                "final": {{ "type": "direct" }}
            }}
        }}"#
    ))
    .expect("parse config");

    let engine = Engine::new(config).expect("build engine");
    let handle = spawn_engine(engine);

    wait_for_listener(proxy_port).await;

    // Direct inbound — no handshake, just raw TCP.
    let mut client = TcpStream::connect(("127.0.0.1", proxy_port))
        .await
        .expect("connect proxy");

    client.write_all(b"ping").await.expect("write payload");
    let mut echoed = [0_u8; 4];
    client.read_exact(&mut echoed).await.expect("read payload");
    assert_eq!(&echoed, b"ping");

    // Verify session tracked.
    let status = handle.export_status();
    assert!(status
        .runtime
        .active_sessions
        .iter()
        .any(|s| s.inbound_tag.as_deref() == Some("direct-in")));

    handle.shutdown().await.expect("shutdown");
    echo_task.abort();
}

#[tokio::test]
async fn direct_inbound_appears_in_config_export() {
    let proxy_port = free_port();

    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "direct-in",
                    "listen": {{ "address": "127.0.0.1", "port": {proxy_port} }},
                    "protocol": {{
                        "type": "direct",
                        "target": "10.0.0.1",
                        "port": 8080
                    }}
                }}
            ],
            "route": {{
                "rules": [],
                "final": {{ "type": "direct" }}
            }}
        }}"#
    ))
    .expect("parse config");

    let engine = Engine::new(config).expect("build engine");
    let handle = spawn_engine(engine);

    wait_for_listener(proxy_port).await;

    let exported = handle.export_status();
    assert_eq!(exported.config.listeners.len(), 1);
    assert_eq!(exported.config.listeners[0].tag, "direct-in");
    assert_eq!(exported.config.listeners[0].protocol, "direct");

    handle.shutdown().await.expect("shutdown");
}
