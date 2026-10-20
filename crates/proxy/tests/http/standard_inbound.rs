use super::*;
use zero_api::{event_type, EventFilter};

#[tokio::test]
async fn standard_connect_keeps_real_identity_authentication_policy_payload_and_half_close() {
    for protocol in ["http", "mixed"] {
        let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = echo.local_addr().unwrap();
        let echo_task = tokio::spawn(async move {
            let (mut io, _) = echo.accept().await.unwrap();
            let mut payload = Vec::new();
            io.read_to_end(&mut payload).await.unwrap();
            assert_eq!(payload, b"original payload");
            io.write_all(&payload).await.unwrap();
            io.shutdown().await.unwrap();
        });
        let proxy_port = free_port();
        let config = RuntimeConfig::parse(
            &serde_json::json!({
                "inbounds": [{"tag":"http-in", "listen":{"address":"127.0.0.1","port":proxy_port},
                    "protocol":{"type":protocol}}],
                "route":{"rules":[],"final":{"type":"direct"}}
            })
            .to_string(),
        )
        .unwrap();
        let engine = Engine::new(config).unwrap();
        let probe = engine.clone();
        let before = probe.engine().mint_session_identity().unwrap();
        let running = spawn_engine(engine);
        wait_for_listener(proxy_port).await;
        let mut client = TcpStream::connect(("127.0.0.1", proxy_port)).await.unwrap();
        let client_address = client.local_addr().unwrap();
        let request =
            format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\noriginal payload");
        client.write_all(request.as_bytes()).await.unwrap();
        client.shutdown().await.unwrap();
        let mut reply = [0; 39];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"HTTP/1.1 200 Connection Established\r\n\r\n");
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"original payload");
        echo_task.await.unwrap();
        wait_for("standard CONNECT settlement", || {
            !probe.completed_sessions().is_empty()
        })
        .await;
        let completed = probe.completed_sessions();
        assert_eq!(completed.len(), 1);
        let record = &completed[0];
        assert_eq!(record.id, before.id().0 + 1);
        assert_eq!(
            probe.engine().mint_session_identity().unwrap().id().0,
            record.id + 1
        );
        assert_eq!(record.source_port, Some(client_address.port()));
        assert_eq!(record.protocol.as_str(), "http");
        assert_eq!(record.bytes_up, 16);
        assert_eq!(record.bytes_down, 16);
        assert!(record.auth.is_none());
        let events = probe
            .latest(
                usize::MAX,
                EventFilter {
                    event_types: vec![
                        event_type::FLOW_STARTED.into(),
                        event_type::FLOW_COMPLETED.into(),
                    ],
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|event| event.principal_key.is_none()));
        running.shutdown().await.unwrap();
    }
}
