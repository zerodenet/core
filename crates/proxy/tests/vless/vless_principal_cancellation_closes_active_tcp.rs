use super::*;

#[tokio::test]
async fn vless_principal_cancellation_closes_active_tcp() {
    let upstream_port = free_port();
    let proxy_port = free_port();
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let upstream = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", upstream_port))
            .await
            .expect("bind upstream");
        let (mut stream, _) = listener.accept().await.expect("accept upstream");
        let _ = accepted_tx.send(());
        let mut buffer = [0u8; 1];
        let read = stream.read(&mut buffer).await.expect("read upstream EOF");
        assert_eq!(read, 0);
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
                        "principal_key": "account:1"
                    }}]
                }}
            }}],
            "outbounds": [],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#
    ))
    .expect("parse config");
    let engine = Engine::new(config).expect("build engine");
    let running = spawn_engine(engine);
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
    accepted_rx.await.expect("upstream accepted");
    wait_for("active VLESS session", || {
        !running.active_sessions().is_empty()
    })
    .await;

    let cancelled = running.close_principal_flows("account:1", "principal_disabled");
    assert_eq!(cancelled.len(), 1);
    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(std::time::Duration::from_secs(2), client.read(&mut byte))
        .await
        .expect("client close timeout")
        .expect("client read");
    assert_eq!(read, 0);
    wait_for("cancelled VLESS session", || {
        running
            .completed_sessions()
            .iter()
            .any(|record| record.id == cancelled[0])
    })
    .await;
    let completed = running
        .completed_sessions()
        .into_iter()
        .find(|record| record.id == cancelled[0])
        .expect("cancelled record");
    assert_eq!(completed.outcome, zero_engine::SessionOutcome::Cancelled);
    assert_eq!(
        completed.close_reason.as_deref(),
        Some("principal_disabled")
    );

    running.shutdown().await.expect("shutdown proxy");
    upstream.await.expect("upstream task");
}
