use super::*;
use crate::{
    validation::{ReplayPolicy, StateLimits},
    CipherKind, ShadowsocksTcpConnectConfig,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::{timeout, Duration},
};
#[path = "wire.rs"]
mod wire;
use wire::*;

#[tokio::test]
async fn facts_path_keeps_first_plaintext_tail_nonces_and_original_response_binding_on_native_tcp()
{
    timeout(Duration::from_secs(5), async {
        for case in cases() {
            let acceptor = ShadowsocksInboundTcpAcceptor::new(profile(case, None));
            let (bytes, session) = request(case, 11);
            let (server, client) = connect_wire(&bytes).await;
            let (destination, auth, mut stream) =
                acceptor.accept_stream_facts(server).await.unwrap();
            assert_eq!(
                destination.address,
                zero_core::Address::Domain("facts.example".into())
            );
            assert_eq!(destination.port, 443);
            assert_eq!(auth.principal_key.as_deref(), Some("facts:user"));
            let mut payload = Vec::new();
            stream.read_to_end(&mut payload).await.unwrap();
            assert_eq!(payload, b"initial-tail");
            stream.write_all(b"reverse").await.unwrap();
            stream.shutdown().await.unwrap();
            let config =
                ShadowsocksTcpConnectConfig::from_config(case.method, &case.password()).unwrap();
            let mut client = config.wrap_outbound_stream(client, session);
            let mut response = Vec::new();
            client.read_to_end(&mut response).await.unwrap();
            assert_eq!(
                response, b"reverse",
                "original request salt must bind the actual 2022 response"
            );
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn facts_and_legacy_clone_entry_share_one_original_replay_authority_after_stream_drop() {
    timeout(Duration::from_secs(5), async {
        for case in cases() {
            let acceptor = ShadowsocksInboundTcpAcceptor::new(profile(case, None));
            let clone = acceptor.clone();
            assert!(Arc::ptr_eq(
                &acceptor.tcp_state.replay_pool,
                &clone.tcp_state.replay_pool
            ));
            let (bytes, _) = request(case, 17);
            let (server, _client) = connect_wire(&bytes).await;
            let (_, _, stream) = acceptor.accept_stream_facts(server).await.unwrap();
            drop(stream);
            let (server, _client) = connect_wire(&bytes).await;
            let failure = clone
                .accept_stream(server)
                .await
                .err()
                .expect("legacy entry cannot bypass facts replay history");
            assert!(failure.to_string().contains("replay"));
            let (server, _client) = connect_wire(&bytes).await;
            let failure = acceptor
                .accept_stream_facts(server)
                .await
                .err()
                .expect("same facts replay history persists");
            assert!(failure.to_string().contains("replay"));
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn facts_path_retains_live_2022_salt_and_capacity_across_clone_failure_and_stream_drop() {
    timeout(Duration::from_secs(5), async {
        for case in cases().into_iter().filter(|case| case.cipher.is_blake3()) {
            let acceptor = ShadowsocksInboundTcpAcceptor::new(profile(case, Some(1)));
            let clone = acceptor.clone();
            let (original, _) = request(case, 23);
            let (server, _client) = connect_wire(&original).await;
            let (_, _, stream) = acceptor.accept_stream_facts(server).await.unwrap();
            drop(stream);
            let (fresh, _) = request(case, 29);
            let (server, _client) = connect_wire(&fresh).await;
            let failure = clone.accept_stream_facts(server).await.err().unwrap();
            assert!(failure.to_string().contains("capacity"));
            let (server, _client) = connect_wire(&original).await;
            let failure = clone.accept_stream_facts(server).await.err().unwrap();
            assert!(
                failure.to_string().contains("replay"),
                "capacity error cannot evict a live original salt"
            );
        }
    })
    .await
    .unwrap();
}

fn profile(case: Case, capacity: Option<usize>) -> ShadowsocksInboundProfile {
    ShadowsocksInboundProfile::from_config_users_with_identity(
        case.method,
        case.identity,
        [ShadowsocksInboundUserRef {
            password: case.user,
            principal_key: Some("facts:user"),
            up_bps: None,
            down_bps: None,
            device_limit: None,
            quota_remaining_bytes: None,
            policy_revision: Some(1),
        }],
    )
    .unwrap()
    .with_replay_policy(ReplayPolicy::Reject)
    .with_state_limits(StateLimits {
        tcp_replay_capacity: capacity,
        ..Default::default()
    })
}
