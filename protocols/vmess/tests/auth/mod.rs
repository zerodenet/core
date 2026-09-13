use super::*;
use crate::{
    crypto::{
        create_xray_auth_id, decode_xray_auth_id, derive_xray_cmd_key, seal_xray_aead_header,
    },
    inbound::{VmessInbound, VmessInboundProfile, VmessUser},
};

mod socket;
use socket::{target, Socket};

fn user(id: u8) -> VmessUser {
    VmessUser {
        id: [id; 16],
        cipher: crate::VmessCipher::Aes128Gcm,
        principal_key: None,
        up_bps: None,
        down_bps: None,
        device_limit: None,
        quota_remaining_bytes: None,
        policy_revision: None,
    }
}

async fn request(user: &VmessUser) -> Vec<u8> {
    let mut socket = Socket::new(vec![]);
    crate::outbound::VmessOutbound
        .establish_tcp_session(&mut socket, &target(), &user.id, user.cipher)
        .await
        .unwrap();
    let bytes = socket.output.lock().unwrap().clone();
    bytes
}

#[tokio::test]
async fn replay_is_rejected_across_profile_clones_replacement_and_stateless_apis() {
    let user = user(1);
    let wire = request(&user).await;
    let profile = VmessInboundProfile::from_users(vec![user.clone()]);
    assert!(profile
        .accept_tcp_stream(VmessInbound, Socket::new(wire.clone()))
        .await
        .is_ok());
    let cloned = profile.clone();
    cloned.replace_users(vec![user.clone()]);
    assert!(cloned
        .accept_tcp_stream(VmessInbound, Socket::new(wire.clone()))
        .await
        .is_err());
    let replacement = VmessInboundProfile::from_users(vec![user.clone()]);
    assert!(replacement
        .accept_tcp_stream(VmessInbound, Socket::new(wire.clone()))
        .await
        .is_err());
    assert!(VmessInbound
        .accept_tcp_with_auth(&mut Socket::new(wire.clone()), &user)
        .await
        .is_err());
    assert!(VmessInbound
        .accept_tcp_with_auth_multi(&mut Socket::new(wire), std::slice::from_ref(&user))
        .await
        .is_err());
    assert!(cloned
        .accept_tcp_stream(VmessInbound, Socket::new(request(&user).await))
        .await
        .is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_replays_accept_exactly_one_handshake() {
    let user = user(2);
    let wire = request(&user).await;
    let profile = VmessInboundProfile::from_users(vec![user]);
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let profile = profile.clone();
        let wire = wire.clone();
        tasks.spawn(async move {
            profile
                .accept_tcp_stream(VmessInbound, Socket::new(wire))
                .await
                .is_ok()
        });
    }
    let mut accepted = 0;
    while let Some(result) = tasks.join_next().await {
        accepted += usize::from(result.unwrap());
    }
    assert_eq!(accepted, 1);
}

#[tokio::test]
async fn valid_authid_with_truncated_header_cannot_be_reused() {
    let user = user(3);
    let wire = request(&user).await;
    assert!(VmessInbound
        .accept_tcp_with_auth(&mut Socket::new(wire[..16].to_vec()), &user)
        .await
        .is_err());
    assert!(VmessInbound
        .accept_tcp_with_auth(&mut Socket::new(wire), &user)
        .await
        .is_err());
}

#[tokio::test]
async fn multi_user_inbound_matches_authenticated_identity() {
    let expected = user(4);
    let wire = request(&expected).await;
    let mut wrong = Socket::new(wire.clone());
    assert!(VmessInbound
        .accept_tcp_with_auth(&mut wrong, &user(5))
        .await
        .is_err());
    let profile = VmessInboundProfile::from_users(vec![user(5), expected]);
    assert!(profile
        .accept_tcp_stream(VmessInbound, Socket::new(wire))
        .await
        .is_ok());
}

#[tokio::test]
async fn stale_future_and_invalid_checksum_authids_are_rejected_before_payload() {
    let user = user(6);
    let key = derive_xray_cmd_key(&user.id);
    let now = crate::crypto::current_timestamp();
    for timestamp in [1, now - 121, now + 300, u64::MAX] {
        let auth = create_xray_auth_id(&key, timestamp).unwrap();
        let mut socket = Socket::new(auth.to_vec());
        let error = VmessInbound
            .accept_tcp_with_auth(&mut socket, &user)
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("timestamp"), "{error}");
        assert!(socket.output.lock().unwrap().is_empty());
    }
    let mut invalid = create_xray_auth_id(&key, now).unwrap();
    invalid[0] ^= 0x80;
    assert!(decode_xray_auth_id(&key, &invalid).is_err());
    let mut socket = Socket::new(invalid.to_vec());
    assert!(VmessInbound
        .accept_tcp_with_auth(&mut socket, &user)
        .await
        .is_err());
}

#[tokio::test]
async fn stale_authid_with_fully_valid_aead_payload_is_rejected() {
    let user = user(7);
    let wire = request(&user).await;
    let key = derive_xray_cmd_key(&user.id);
    let auth = wire[..16].try_into().unwrap();
    let nonce = wire[34..42].try_into().unwrap();
    let body =
        crate::crypto::open_xray_aead_header_payload(&key, &auth, &nonce, &wire[42..]).unwrap();
    let stale = create_xray_auth_id(&key, 1).unwrap();
    let wire = seal_xray_aead_header(&key, &stale, &body).unwrap();
    assert!(VmessInbound
        .accept_tcp_with_auth(&mut Socket::new(wire), &user)
        .await
        .is_err());
}

#[test]
fn time_window_is_inclusive_and_rejects_negative_wire_timestamps() {
    for timestamp in [880, 1000, 1120] {
        assert!(validate_time(timestamp, 1000).is_ok());
    }
    for timestamp in [879, 1121, u64::MAX] {
        assert!(validate_time(timestamp, 1000).is_err());
    }
}

#[test]
fn replay_retention_covers_future_ids_and_capacity_never_evicts_live_ids() {
    let mut cache = ReplayCache::default();
    let now = Instant::now();
    let identity = ([1; 16], [2; 16]);
    cache.accept(identity, now, 1).unwrap();
    assert!(cache.accept(([3; 16], [4; 16]), now, 1).is_err());
    assert!(cache
        .accept(identity, now + Duration::from_secs(240), 1)
        .is_err());
    assert!(cache.accept(identity, now + RETENTION, 1).is_ok());
    assert_eq!(cache.seen.len(), 1);
    assert_eq!(cache.observed.len(), 1);
}

#[test]
fn replay_identity_includes_credential() {
    let mut cache = ReplayCache::default();
    let now = Instant::now();
    cache.accept(([1; 16], [9; 16]), now, 2).unwrap();
    cache.accept(([2; 16], [9; 16]), now, 2).unwrap();
}
