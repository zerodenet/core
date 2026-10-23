#![cfg(feature = "runtime")]

use trojan::inbound::TrojanInboundProfileStore;
use trojan::transport::TrojanInboundUserRef;

#[test]
fn transport_runtime_atomically_replaces_shared_inbound_users() {
    let profiles = TrojanInboundProfileStore::default();
    let old_users = [TrojanInboundUserRef {
        password: "old-secret",
        principal_key: Some("account:old"),
        up_bps: None,
        down_bps: None,
        device_limit: None,
        quota_remaining_bytes: None,
        policy_revision: None,
    }];
    let profile = profiles.replace("trojan-in", &old_users);
    assert_eq!(profile.user_count(), 1);

    let new_users = [TrojanInboundUserRef {
        password: "new-secret",
        principal_key: Some("account:new"),
        up_bps: Some(1_000),
        down_bps: Some(2_000),
        device_limit: Some(2),
        quota_remaining_bytes: Some(4096),
        policy_revision: Some(2),
    }];
    let replacement = profiles.replace("trojan-in", &new_users);
    assert_eq!(profile.user_count(), 1);
    assert_eq!(replacement.user_count(), 1);

    profiles.replace("trojan-in", &[]);
    assert_eq!(profile.user_count(), 0);
}

fn user<'a>(password: &'a str, principal: &'a str, revision: u64) -> TrojanInboundUserRef<'a> {
    TrojanInboundUserRef {
        password,
        principal_key: Some(principal),
        up_bps: Some(1_000),
        down_bps: Some(2_000),
        device_limit: Some(2),
        quota_remaining_bytes: Some(4096),
        policy_revision: Some(revision),
    }
}

fn request(password: &str) -> Vec<u8> {
    let hash = match password {
        "test-password" => "b42338ef718bb33b88d3c2bf0cb6130499a1dc05e5c101b3722b09c7",
        "wrong-password" => "c860da892e31176af374c37fb20599f70f6d5428c0e53f45a3787c0e",
        _ => panic!("fixture password"),
    };
    let mut bytes = hash.as_bytes().to_vec();
    bytes.extend_from_slice(b"\r\n\x01\x01\x7f\x00\x00\x01\x01\xbb\r\n");
    bytes
}

async fn sockets() -> (tokio::net::TcpStream, zero_platform_tokio::TokioSocket) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (client, server) = tokio::join!(
        tokio::net::TcpStream::connect(listener.local_addr().unwrap()),
        listener.accept(),
    );
    (
        client.unwrap(),
        zero_platform_tokio::TokioSocket::new(server.unwrap().0),
    )
}

async fn authenticate(
    profile: &trojan::inbound::TrojanInboundProfile,
    password: &str,
) -> Result<trojan::inbound::TrojanRequestFacts, zero_core::Error> {
    use tokio::io::AsyncWriteExt;
    let (mut client, mut server) = sockets().await;
    client.write_all(&request(password)).await.unwrap();
    client.shutdown().await.unwrap();
    profile
        .accept_request_facts(trojan::inbound::TrojanInbound, &mut server)
        .await
}

#[tokio::test]
async fn cold_updates_do_not_publish_until_commit_and_keep_actual_inflight_auth_snapshot() {
    use std::{future::Future, task::Poll};
    use tokio::io::AsyncWriteExt;
    use trojan::inbound::TrojanInbound;

    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let profiles = TrojanInboundProfileStore::default();
        let old_users = [user("test-password", "account:old", 1)];
        let new_users = [user("wrong-password", "account:new", 2)];
        let profile = profiles.replace("in", &old_users);
        let original_clone = profile.clone();
        let discarded = profiles.prepare("in", &new_users);
        assert!(authenticate(&profile, "wrong-password").await.is_err());
        assert_eq!(
            authenticate(&profile, "test-password")
                .await
                .unwrap()
                .auth
                .policy_revision,
            Some(1)
        );
        drop(discarded);
        assert_eq!(
            authenticate(&profile, "test-password")
                .await
                .unwrap()
                .auth
                .principal_key
                .as_deref(),
            Some("account:old")
        );

        let staged = profiles.prepare("in", &new_users);
        let (mut client, mut server) = sockets().await;
        let mut pending = Box::pin(profile.accept_request_facts(TrojanInbound, &mut server));
        // Actual socket read is Pending after the profile's original users were snapshotted.
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let installed = staged.commit();
        client.write_all(&request("test-password")).await.unwrap();
        let auth = pending.await.unwrap().auth;
        assert_eq!(auth.principal_key.as_deref(), Some("account:old"));
        assert_eq!(auth.policy_revision, Some(1));
        for same_profile in [&profile, &original_clone, &installed] {
            assert!(authenticate(same_profile, "test-password").await.is_err());
            let auth = authenticate(same_profile, "wrong-password")
                .await
                .unwrap()
                .auth;
            assert_eq!(auth.scheme, "trojan");
            assert_eq!(auth.principal_key.as_deref(), Some("account:new"));
            assert_eq!((auth.up_bps, auth.down_bps), (Some(1_000), Some(2_000)));
            assert_eq!(auth.device_limit, Some(2));
            assert_eq!(auth.quota_remaining_bytes, Some(4096));
            assert_eq!(auth.policy_revision, Some(2));
        }
    })
    .await
    .unwrap();
}
