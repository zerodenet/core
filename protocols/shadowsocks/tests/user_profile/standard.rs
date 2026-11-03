//! Cold publication, packet authentication and original association return facts.
use super::*;

#[tokio::test]
async fn standard_profile_prepare_drop_and_invalid_candidate_do_not_publish_live_users() {
    let store = ShadowsocksInboundProfileStore::default();
    let initial = users();
    let live = store
        .replace("standard", "aes-128-gcm", &initial[..1])
        .unwrap();
    let prepare = || {
        store
            .prepare_with_identity(
                "standard",
                "aes-128-gcm",
                None,
                &initial,
                Default::default(),
                Default::default(),
            )
            .unwrap()
    };
    let dropped = prepare();
    assert_eq!(live.user_count(), 1);
    drop(dropped);
    assert_eq!(live.user_count(), 1);
    assert!(store
        .prepare_with_identity(
            "standard",
            "invalid-cipher",
            None,
            &initial,
            Default::default(),
            Default::default()
        )
        .is_err());
    assert_eq!(live.user_count(), 1);
    let current = prepare().activate();
    assert_eq!(live.user_count(), 2);
    store
        .replace("standard", "aes-128-gcm", &initial[..1])
        .unwrap();
    assert_eq!(current.user_count(), 1, "same actual live user authority");
}

#[tokio::test]
async fn standard_udp_responses_use_packet_user_association_after_interleaved_authentication() {
    let configured = users();
    let profile = ShadowsocksInboundProfile::from_config_users("aes-128-gcm", configured).unwrap();
    let mut responder = ShadowsocksInboundUdpResponder::from_profile(profile.clone());
    let target = Address::Ipv4([127, 0, 0, 1]);
    let sender = "127.0.0.1:12345".parse().unwrap();
    let a = ShadowsocksDatagramCodec::new(shadowsocks::CipherKind::Aes128Gcm, "first-secret");
    let b = ShadowsocksDatagramCodec::new(shadowsocks::CipherKind::Aes128Gcm, "second-secret");
    let (first, first_auth) = responder
        .decode_authenticated_packet(&a.encode(&target, 53, b"first").unwrap(), sender)
        .unwrap();
    let (second, second_auth) = responder
        .decode_authenticated_packet(&b.encode(&target, 53, b"second").unwrap(), sender)
        .unwrap();
    assert_ne!(first.client_session_id(), second.client_session_id());
    assert_eq!(first_auth.principal_key.as_deref(), Some("principal-1"));
    assert_eq!(second_auth.principal_key.as_deref(), Some("principal-2"));
    assert_eq!(first_auth.policy_revision, Some(1));
    assert_eq!(second_auth.quota_remaining_bytes, Some(4096));
    let (client, encoded) = responder
        .encode_association_response(first.client_session_id().unwrap(), &target, 53, b"reply-a")
        .unwrap();
    assert_eq!(client, sender);
    assert_eq!(a.decode(&encoded).unwrap().2, b"reply-a");
    assert!(b.decode(&encoded).is_none());
    profile
        .replace_config_users(configured[..1].iter().copied())
        .unwrap();
    assert!(responder
        .encode_association_response(
            second.client_session_id().unwrap(),
            &target,
            53,
            b"retired-user"
        )
        .is_err());
    assert!(responder
        .encode_association_response(u64::MAX, &target, 53, b"unknown")
        .is_err());
}

#[cfg(feature = "blake3")]
#[tokio::test]
async fn standard_sip023_udp_association_keeps_original_crypto_and_latest_authenticated_endpoint() {
    for cipher in [
        shadowsocks::CipherKind::Blake3Aes128Gcm,
        shadowsocks::CipherKind::Blake3Aes256Gcm,
    ] {
        let (identity, user) = eih_keys(cipher);
        let method = if cipher == shadowsocks::CipherKind::Blake3Aes128Gcm {
            "2022-blake3-aes-128-gcm"
        } else {
            "2022-blake3-aes-256-gcm"
        };
        let profile = ShadowsocksInboundProfile::from_config_users_with_identity(
            method,
            Some(identity),
            [eih_user(cipher, user)],
        )
        .unwrap();
        let mut responder = ShadowsocksInboundUdpResponder::from_profile(profile);
        let outbound = ShadowsocksDatagramCodec::new(cipher, format!("{identity}:{user}"));
        let target = Address::Ipv4([127, 0, 0, 1]);
        let a = "127.0.0.1:12345".parse().unwrap();
        let b = "127.0.0.1:12346".parse().unwrap();
        let wire = outbound.encode(&target, 53, b"first").unwrap();
        let (first, _) = responder.decode_authenticated_packet(&wire, a).unwrap();
        assert!(
            responder.decode_authenticated_packet(&wire, b).is_err(),
            "replay does not move return endpoint"
        );
        let id = first.client_session_id().unwrap();
        assert_eq!(
            responder
                .encode_association_response(id, &target, 53, b"before")
                .unwrap()
                .0,
            a
        );
        let (moved, _) = responder
            .decode_authenticated_packet(&outbound.encode(&target, 54, b"move").unwrap(), b)
            .unwrap();
        assert_eq!(moved.client_session_id(), Some(id));
        let (client, wire) = responder
            .encode_association_response(id, &target, 53, b"reply-from-old-target")
            .unwrap();
        assert_eq!(client, b);
        assert_eq!(outbound.decode(&wire).unwrap().2, b"reply-from-old-target");
    }
}
