#![cfg(feature = "crypto")]

use hysteria2::inbound::{Hysteria2InboundProfileStore, Hysteria2InboundUserRef};

fn user<'a>(
    password: &'a str,
    principal_key: &'a str,
    up_bps: u64,
    down_bps: u64,
) -> Hysteria2InboundUserRef<'a> {
    Hysteria2InboundUserRef {
        password,
        principal_key: Some(principal_key),
        up_bps: Some(up_bps),
        down_bps: Some(down_bps),
        device_limit: Some(2),
        quota_remaining_bytes: Some(4096),
        policy_revision: Some(2),
    }
}

#[test]
fn profile_store_matches_users_and_updates_existing_profile() {
    let store = Hysteria2InboundProfileStore::default();
    let users = [
        user("first-secret", "principal-1", 10, 20),
        user("second-secret", "principal-2", 30, 40),
    ];
    let profile = store.replace("hy2-in", &users);
    let salt = [7_u8; 32];
    let hmac = hysteria2::shared::sign_hmac("second-secret", &salt);
    let auth = profile.authenticate_hmac(&salt, &hmac).unwrap();
    assert_eq!(auth.principal_key.as_deref(), Some("principal-2"));
    assert_eq!(auth.up_bps, Some(30));
    assert_eq!(auth.down_bps, Some(40));
    assert_eq!(auth.device_limit, Some(2));
    assert_eq!(auth.quota_remaining_bytes, Some(4096));
    assert_eq!(auth.policy_revision, Some(2));

    let replacement = [user("new-secret", "principal-new", 50, 60)];
    store.replace("hy2-in", &replacement);
    assert!(profile.authenticate_hmac(&salt, &hmac).is_err());
    let new_hmac = hysteria2::shared::sign_hmac("new-secret", &salt);
    let auth = profile.authenticate_hmac(&salt, &new_hmac).unwrap();
    assert_eq!(auth.principal_key.as_deref(), Some("principal-new"));

    let empty: [Hysteria2InboundUserRef<'_>; 0] = [];
    store.replace("hy2-in", &empty);
    assert_eq!(profile.user_count(), 0);
    assert!(profile.authenticate_hmac(&salt, &new_hmac).is_err());
}
