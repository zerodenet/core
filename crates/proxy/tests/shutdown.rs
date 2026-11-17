use crate::support;

use zero_config::RuntimeConfig;
use zero_proxy::Proxy as Engine;

use support::{free_port, spawn_engine, wait_for_listener};

#[tokio::test(flavor = "current_thread")]
async fn stops_cleanly_when_shutdown_signal_arrives() {
    let proxy_port = free_port();
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
    .expect("parse engine config");

    let engine = Engine::new(config).expect("build engine");
    let engine_handle = spawn_engine(engine);

    // This current-thread caller has not yielded to the real startup yet.
    // An expired readiness observer must leave that same startup/Scope owned.
    let expired = tokio::time::Instant::now() - std::time::Duration::from_secs(1);
    assert!(engine_handle.wait_ready(expired).await.is_err());
    engine_handle
        .wait_ready(tokio::time::Instant::now() + std::time::Duration::from_secs(3))
        .await
        .expect("actual Source readiness and committed component generation");

    wait_for_listener(proxy_port).await;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut client = tokio::net::TcpStream::connect(("127.0.0.1", proxy_port))
        .await
        .unwrap();
    client.write_all(&[5, 1, 0]).await.unwrap();
    let mut reply = [0; 2];
    client.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply, [5, 0]);
    drop(client);

    engine_handle.shutdown().await.expect("shutdown engine");
}
