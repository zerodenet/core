use super::*;

#[tokio::test]
async fn vless_shared_quota_exhaustion_closes_and_blocks_the_principal() {
    let upstream_port = free_port();
    let proxy_port = free_port();
    let (accepted_tx, mut accepted_rx) = tokio::sync::mpsc::unbounded_channel();
    let upstream = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", upstream_port))
            .await
            .expect("bind upstream");
        let (mut stream, _) = listener.accept().await.expect("accept upstream");
        accepted_tx.send(()).expect("report upstream accept");
        let mut buffer = Vec::new();
        let _ = stream.read_to_end(&mut buffer).await;
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
                        "quota_remaining_bytes": 4,
                        "policy_revision": 1
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

    let mut client = TcpStream::connect(("127.0.0.1", proxy_port))
        .await
        .expect("connect proxy");
    client
        .write_all(&vless_request_for_ipv4(
            USER_ID,
            [127, 0, 0, 1],
            upstream_port,
        ))
        .await
        .expect("write VLESS request");
    let mut response = [0u8; 2];
    client
        .read_exact(&mut response)
        .await
        .expect("read VLESS response");
    assert_eq!(response, [0, 0]);
    accepted_rx.recv().await.expect("upstream accepted");

    client
        .write_all(b"12345")
        .await
        .expect("write quota payload");
    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(std::time::Duration::from_secs(2), client.read(&mut byte))
        .await
        .expect("quota close timeout")
        .expect("client read");
    assert_eq!(read, 0);
    wait_for("quota-exhausted flow", || {
        running
            .completed_sessions()
            .iter()
            .any(|record| record.close_reason.as_deref() == Some("quota_exhausted"))
    })
    .await;

    let mut rejected = TcpStream::connect(("127.0.0.1", proxy_port))
        .await
        .expect("reconnect proxy");
    rejected
        .write_all(&vless_request_for_ipv4(
            USER_ID,
            [127, 0, 0, 1],
            upstream_port,
        ))
        .await
        .expect("write reconnect request");
    let mut rejected_response = [0u8; 2];
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        rejected.read_exact(&mut rejected_response),
    )
    .await
    .expect("quota reconnect rejection timeout");
    assert!(result.is_err(), "exhausted revision was admitted again");

    running.shutdown().await.expect("shutdown proxy");
    upstream.await.expect("upstream task");
}
