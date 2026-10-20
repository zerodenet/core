//! The existing post-reconcile hot callback remains a real publishing path.
use super::*;
const NEW_USER_ID: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

#[tokio::test]
async fn acknowledged_vmess_credential_reload_preserves_the_live_tls_listener_and_updates_auth() {
    timeout(Duration::from_secs(10), async {
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = target.local_addr().unwrap();
        let worker = tokio::spawn(async move {
            for expected in [b'1', b'2'] {
                let (mut io, _) = target.accept().await.unwrap();
                let mut request = Vec::new();
                io.read_to_end(&mut request).await.unwrap();
                assert_eq!(request, [expected]);
                io.write_all(&[expected]).await.unwrap();
                io.shutdown().await.unwrap();
            }
        });
        let fixture = Fixture::new(Some("vmess:old"), false);
        exchange(&fixture, address, USER_ID, b'1').await;
        wait_for("old VMess request settlement", || {
            fixture.probe.completed_sessions().len() == 1
        })
        .await;
        // A credentials-only apply must retain the actual listener/acceptor.
        // Reconstructing it would fail the original certificate file loading.
        fixture.remove_tls_files();
        let handle = zero_proxy::ProxyHandle::new(
            zero_engine::EngineHandle::new(fixture.probe.engine().clone()),
            fixture.probe.clone(),
        );
        let mut candidate = (*fixture.probe.engine().config()).clone();
        let zero_config::InboundProtocolConfig::Vmess { users, .. } =
            &mut candidate.inbounds[0].protocol
        else {
            unreachable!()
        };
        users[0].id = NEW_USER_ID.into();
        users[0].principal_key = Some("vmess:new".into());
        users[0].up_bps = Some(8192);
        users[0].down_bps = Some(16384);
        users[0].device_limit = Some(3);
        users[0].quota_remaining_bytes = Some(2048);
        users[0].policy_revision = Some(8);
        handle
            .apply_runtime_config_and_wait(candidate, Duration::from_secs(5))
            .await
            .unwrap();
        exchange(&fixture, address, NEW_USER_ID, b'2').await;
        worker.await.unwrap();
        wait_for("replacement VMess request settlement", || {
            fixture.probe.completed_sessions().len() == 2
        })
        .await;
        let completed = fixture.probe.completed_sessions();
        let auth = completed
            .iter()
            .find_map(|record| {
                record
                    .auth
                    .as_ref()
                    .filter(|auth| auth.principal_key.as_deref() == Some("vmess:new"))
            })
            .unwrap();
        assert_eq!((auth.up_bps, auth.down_bps), (Some(8192), Some(16384)));
        assert_eq!(auth.device_limit, Some(3));
        assert_eq!(auth.quota_remaining_bytes, Some(2048));
        assert_eq!(auth.policy_revision, Some(8));
        let (mut removed, _) = vmess_client_for(&fixture, address, USER_ID).await;
        let mut byte = [0];
        assert!(
            !matches!(removed.read(&mut byte).await, Ok(n) if n != 0),
            "removed credentials must not receive an authenticated response"
        );
        assert_eq!(
            fixture.probe.completed_sessions().len(),
            2,
            "rejected authentication creates no business Session"
        );
        fixture.running.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
async fn exchange(fixture: &Fixture, address: std::net::SocketAddr, id: &str, payload: u8) {
    let (mut client, _) = vmess_client_for(fixture, address, id).await;
    client.write_all(&[payload]).await.unwrap();
    client.shutdown().await.unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).await.unwrap();
    assert_eq!(response, [payload]);
}
