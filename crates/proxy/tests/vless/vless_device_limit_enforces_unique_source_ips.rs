use super::*;
use tokio::net::TcpSocket;

async fn connect_from(proxy_port: u16, source_ip: [u8; 4]) -> TcpStream {
    let socket = TcpSocket::new_v4().expect("create client socket");
    socket
        .bind(std::net::SocketAddr::from((source_ip, 0)))
        .expect("bind client source address");
    socket
        .connect(std::net::SocketAddr::from(([127, 0, 0, 1], proxy_port)))
        .await
        .expect("connect proxy")
}

async fn authenticate(client: &mut TcpStream, upstream_port: u16) -> std::io::Result<[u8; 2]> {
    client
        .write_all(&vless_request_for_ipv4(
            USER_ID,
            [127, 0, 0, 1],
            upstream_port,
        ))
        .await?;
    let mut response = [0u8; 2];
    client.read_exact(&mut response).await?;
    Ok(response)
}

#[tokio::test]
#[cfg_attr(
    target_os = "macos",
    ignore = "macOS requires an explicit loopback alias before binding 127.0.0.2"
)]
async fn vless_device_limit_enforces_unique_source_ips_and_releases_on_close() {
    let upstream_port = free_port();
    let proxy_port = free_port();
    let (accepted_tx, mut accepted_rx) = tokio::sync::mpsc::unbounded_channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let upstream = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", upstream_port))
            .await
            .expect("bind upstream");
        let mut release_rx = Some(release_rx);
        for index in 0..2 {
            let (mut stream, _) = listener.accept().await.expect("accept upstream");
            accepted_tx.send(index).expect("report upstream accept");
            if index == 0 {
                tokio::spawn(async move {
                    let mut buffer = Vec::new();
                    let _ = stream.read_to_end(&mut buffer).await;
                });
            } else {
                let _ = release_rx.take().expect("release receiver").await;
                drop(stream);
            }
        }
    });

    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [{{
                "tag": "vless-in",
                "listen": {{ "address": "127.0.0.1", "port": {proxy_port} }},
                "protocol": {{
                    "type": "vless",
                    "users": [{{
                        "id": "{USER_ID}",
                        "principal_key": "account:1",
                        "device_limit": 1
                    }}]
                }}
            }}],
            "outbounds": [],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#
    ))
    .expect("parse config");
    let running = spawn_engine(Engine::new(config).expect("build proxy"));
    wait_for_listener(proxy_port).await;

    let mut first = connect_from(proxy_port, [127, 0, 0, 1]).await;
    assert_eq!(
        authenticate(&mut first, upstream_port).await.unwrap(),
        [0, 0]
    );
    assert_eq!(accepted_rx.recv().await, Some(0));

    let mut rejected = connect_from(proxy_port, [127, 0, 0, 2]).await;
    let rejection = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        authenticate(&mut rejected, upstream_port),
    )
    .await
    .expect("device-limit rejection timeout");
    assert!(rejection.is_err(), "second source IP was admitted");
    assert!(accepted_rx.try_recv().is_err());

    drop(first);
    wait_for("first device release", || {
        running.active_sessions().is_empty()
    })
    .await;

    let mut replacement = connect_from(proxy_port, [127, 0, 0, 2]).await;
    assert_eq!(
        authenticate(&mut replacement, upstream_port).await.unwrap(),
        [0, 0]
    );
    assert_eq!(accepted_rx.recv().await, Some(1));

    drop(replacement);
    let _ = release_tx.send(());
    running.shutdown().await.expect("shutdown proxy");
    upstream.await.expect("upstream task");
}
