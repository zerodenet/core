use super::*;

#[tokio::test]
async fn credential_only_reload_keeps_same_listener_owner_and_actual_profile_authentication() {
    timeout(Duration::from_secs(6), async {
        let mut fixture = Fixture::new_protocol(
            1,
            serde_json::json!({
                "type":"vless", "users":[{"id":"11111111-1111-1111-1111-111111111111",
                    "principal_key":"account:old", "policy_revision":1}]
            }),
        )
        .await;
        let original = fixture.originals[0].clone();
        let port = fixture.old.inbounds[0].listen.port;
        let mut candidate = (*fixture.old).clone();
        let zero_config::InboundProtocolConfig::Vless { users, .. } =
            &mut candidate.inbounds[0].protocol
        else {
            panic!("VLESS");
        };
        users[0].id = "22222222-2222-2222-2222-222222222222".into();
        users[0].principal_key = Some("account:new".into());
        users[0].policy_revision = Some(2);
        fixture
            .proxy
            .engine
            .stage_runtime_config(candidate)
            .unwrap();
        let candidate = fixture.proxy.engine().config();
        let next = factory(&fixture.proxy);
        let rollback = fixture.rollback.clone();
        let protocols = fixture.proxy.protocols.clone();
        reconcile_inbounds(
            &protocols,
            None,
            &next,
            &rollback,
            &candidate,
            fixture.state(),
        )
        .await
        .unwrap();
        assert!(Arc::ptr_eq(
            &original,
            &fixture.proxy.inbound_owners.owner("old-0").unwrap()
        ));
        assert!(original.is_running());
        assert!(original.terminal().is_none());
        assert!(!fixture.gate.closed.load(Ordering::Acquire));
        assert!(TcpListener::bind(("127.0.0.1", port)).await.is_err());
        // The real post-reconcile callback remains the publication point for
        // protocol hot credentials; no cold operation installed another profile.
        protocols.on_config_reloaded(&candidate);
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_port = target.local_addr().unwrap().port();
        let before = fixture.proxy.engine().mint_session_identity().unwrap();
        let mut removed = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        removed
            .write_all(&request(0x11, target_port))
            .await
            .unwrap();
        let mut response = Vec::new();
        let _closed = removed.read_to_end(&mut response).await;
        assert!(response.is_empty());
        assert_eq!(
            fixture
                .proxy
                .engine()
                .mint_session_identity()
                .unwrap()
                .id()
                .0,
            before.id().0 + 1
        );
        let worker = tokio::spawn(async move {
            let (mut io, _) = target.accept().await.unwrap();
            let mut bytes = Vec::new();
            io.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, b"hot-auth");
            io.write_all(&bytes).await.unwrap();
            io.shutdown().await.unwrap();
        });
        let mut accepted = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let mut bytes = request(0x22, target_port);
        bytes.extend_from_slice(b"hot-auth");
        accepted.write_all(&bytes).await.unwrap();
        accepted.shutdown().await.unwrap();
        let mut response = Vec::new();
        accepted.read_to_end(&mut response).await.unwrap();
        assert_eq!(&response[..2], &[0, 0]);
        assert_eq!(&response[2..], b"hot-auth");
        worker.await.unwrap();
        while fixture.proxy.completed_sessions().is_empty() {
            tokio::task::yield_now().await;
        }
        let completed = fixture.proxy.completed_sessions();
        assert_eq!(completed.len(), 1);
        let auth = completed[0].auth.as_ref().unwrap();
        assert_eq!(auth.scheme, "vless");
        assert_eq!(auth.principal_key.as_deref(), Some("account:new"));
        assert_eq!(auth.policy_revision, Some(2));
        assert_eq!(
            (
                auth.up_bps,
                auth.down_bps,
                auth.device_limit,
                auth.quota_remaining_bytes
            ),
            (None, None, None, None)
        );
        fixture.gate.release.write_u8(1).await.unwrap();
        fixture.finish().await;
    })
    .await
    .unwrap();
}
fn request(id: u8, port: u16) -> Vec<u8> {
    let mut request = vec![0];
    request.extend_from_slice(&[id; 16]);
    request.extend_from_slice(&[0, 1]);
    request.extend_from_slice(&port.to_be_bytes());
    request.extend_from_slice(&[1, 127, 0, 0, 1]);
    request
}
