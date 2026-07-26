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
