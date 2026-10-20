use super::*;

#[tokio::test]
async fn hot_credentials_publish_to_live_trojan_listener_without_restarting_its_actual_stream() {
    timeout(Duration::from_secs(10), async {
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = target.local_addr().unwrap();
        let worker = tokio::spawn(async move {
            let (mut old, _) = target.accept().await.unwrap();
            for expected in [b"before", b"after!"] {
                let mut received = [0; 6];
                old.read_exact(&mut received).await.unwrap();
                assert_eq!(&received, expected);
                old.write_all(expected).await.unwrap();
            }
            let mut tail = Vec::new();
            old.read_to_end(&mut tail).await.unwrap();
            assert!(tail.is_empty());
            old.shutdown().await.unwrap();
            let (mut new, _) = target.accept().await.unwrap();
            let mut payload = Vec::new();
            new.read_to_end(&mut payload).await.unwrap();
            assert_eq!(payload, b"new-auth");
            new.write_all(&payload).await.unwrap();
            new.shutdown().await.unwrap();
        });
        let fixture = Fixture::new(Some("account:old"), false);
        let handle = zero_proxy::ProxyHandle::new(
            zero_engine::EngineHandle::new(fixture.probe.engine().clone()),
            fixture.probe.clone(),
        );
        let (mut old, old_source) = fixture.connect().await;
        let mut bytes = request(PASSWORD, CMD_TCP, address);
        bytes.extend_from_slice(b"before");
        old.write_all(&bytes).await.unwrap();
        let mut echoed = [0; 6];
        old.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"before");

        let mut updated = (*fixture.probe.engine().config()).clone();
        let zero_config::InboundProtocolConfig::Trojan { users, .. } =
            &mut updated.inbounds[0].protocol
        else {
            panic!("Trojan config");
        };
        users[0].password = "wrong-password".into();
        users[0].principal_key = Some("account:new".into());
        users[0].policy_revision = Some(8);
        handle
            .apply_runtime_config_and_wait(updated, Duration::from_secs(5))
            .await
            .unwrap();

        // A listener restart would stop this actual accepted driver. The old
        // authenticated session retains its original immutable auth snapshot.
        old.write_all(b"after!").await.unwrap();
        old.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"after!");
        old.shutdown().await.unwrap();
        let mut tail = Vec::new();
        old.read_to_end(&mut tail).await.unwrap();
        assert!(tail.is_empty(), "Trojan never inserts reply bytes");

        let before_rejection = fixture.probe.engine().mint_session_identity().unwrap();
        let (mut removed, _) = fixture.connect().await;
        removed
            .write_all(&request(PASSWORD, CMD_TCP, address))
            .await
            .unwrap();
        assert_no_response(&mut removed).await;
        let after_rejection = fixture.probe.engine().mint_session_identity().unwrap();
        assert_eq!(after_rejection.id().0, before_rejection.id().0 + 1);

        let (mut new, new_source) = fixture.connect().await;
        let mut bytes = request("wrong-password", CMD_TCP, address);
        bytes.extend_from_slice(b"new-auth");
        new.write_all(&bytes).await.unwrap();
        new.shutdown().await.unwrap();
        let mut response = Vec::new();
        new.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"new-auth");
        worker.await.unwrap();
        wait_for("Trojan hot users settled", || {
            fixture.probe.completed_sessions().len() == 2
        })
        .await;
        let completed = fixture.probe.completed_sessions();
        for (source, principal, revision, bytes) in [
            (old_source, "account:old", 7, 12),
            (new_source, "account:new", 8, 8),
        ] {
            let record = completed
                .iter()
                .find(|record| record.source_port == Some(source.port()))
                .unwrap();
            assert_eq!(record.protocol.as_str(), "trojan");
            assert_eq!((record.bytes_up, record.bytes_down), (bytes, bytes));
            let auth = record.auth.as_ref().unwrap();
            assert_eq!(auth.scheme, "trojan");
            assert_eq!(auth.principal_key.as_deref(), Some(principal));
            assert_eq!((auth.up_bps, auth.down_bps), (Some(65536), Some(32768)));
            assert_eq!(auth.device_limit, Some(2));
            assert_eq!(auth.quota_remaining_bytes, Some(4096));
            assert_eq!(auth.policy_revision, Some(revision));
        }
        fixture.running.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
