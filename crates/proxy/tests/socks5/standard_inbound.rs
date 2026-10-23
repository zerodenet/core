use super::*;

#[tokio::test]
async fn standard_connect_keeps_real_identity_authentication_policy_payload_and_half_close() {
    for (authenticate, principal) in [
        (false, None),
        (true, None),
        (true, Some("configured:alice")),
    ] {
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
        let users = if authenticate {
            serde_json::json!([{
                "username": "alice", "password": "secret", "principal_key": principal,
                "up_bps": 65536, "down_bps": 32768
            }])
        } else {
            serde_json::json!([])
        };
        let config = RuntimeConfig::parse(
            &serde_json::json!({
                "inbounds": [{"tag":"socks-in", "listen":{"address":"127.0.0.1","port":proxy_port},
                    "protocol":{"type":"socks5","users":users}}],
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
        client
            .write_all(&[5, 1, if authenticate { 2 } else { 0 }])
            .await
            .unwrap();
        let mut method = [0; 2];
        client.read_exact(&mut method).await.unwrap();
        assert_eq!(method, [5, if authenticate { 2 } else { 0 }]);
        if authenticate {
            client.write_all(b"\x01\x05alice\x06secret").await.unwrap();
            let mut reply = [0; 2];
            client.read_exact(&mut reply).await.unwrap();
            assert_eq!(reply, [1, 0]);
        }
        let mut request = vec![5, 1, 0, 1, 127, 0, 0, 1];
        request.extend_from_slice(&target.port().to_be_bytes());
        request.extend_from_slice(b"original payload");
        client.write_all(&request).await.unwrap();
        client.shutdown().await.unwrap();
        let mut reply = [0; 10];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(reply, [5, 0, 0, 1, 0, 0, 0, 0, 0, 0]);
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
        assert_eq!(record.protocol.as_str(), "socks5");
        assert_eq!(record.bytes_up, 16);
        assert_eq!(record.bytes_down, 16);
        assert_eq!(record.auth.is_some(), authenticate);
        if let Some(auth) = &record.auth {
            assert_eq!(auth.scheme, "socks5");
            assert_eq!(auth.principal_key.as_deref(), principal);
            assert_eq!((auth.up_bps, auth.down_bps), (Some(65536), Some(32768)));
        }
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
        assert!(events
            .iter()
            .all(|event| event.principal_key.as_deref() == principal));
        running.shutdown().await.unwrap();
    }
}
