//! Actual auth/replay state remains unchanged until explicit publication.
use super::*;
use crate::{
    inbound::PreparedVmessInboundUsers,
    transport::{VmessInboundUserRef, VmessTransportRuntime},
};

fn reference(id: &str) -> VmessInboundUserRef<'_> {
    VmessInboundUserRef {
        id,
        cipher: "aes-128-gcm",
        principal_key: Some("staged:vmess"),
        up_bps: Some(1024),
        down_bps: Some(2048),
        device_limit: Some(2),
        quota_remaining_bytes: Some(4096),
        policy_revision: Some(9),
    }
}
fn stage(
    runtime: &VmessTransportRuntime,
    users: &[VmessInboundUserRef<'_>],
) -> crate::transport::PreparedVmessInboundProfile {
    runtime.stage_inbound_profile(
        "staged",
        PreparedVmessInboundUsers::from_config_users(users.iter().copied()).unwrap(),
    )
}
async fn accepts(profile: &VmessInboundProfile, expected: &VmessUser) -> bool {
    profile
        .accept_request_stream(VmessInbound, Socket::new(request(expected).await))
        .await
        .is_ok()
}
#[tokio::test]
async fn staged_users_do_not_publish_on_prepare_drop_or_invalid_input() {
    let runtime = VmessTransportRuntime::default();
    let old_id = "47474747-4747-4747-4747-474747474747";
    let new_id = "48484848-4848-4848-4848-484848484848";
    let old = user(0x47);
    let new = user(0x48);
    let profile = runtime
        .replace_inbound_profile("staged", &[reference(old_id)])
        .unwrap();
    let pending = stage(&runtime, &[reference(new_id)]);
    assert!(accepts(&profile, &old).await);
    assert!(!accepts(&profile, &new).await);
    drop(pending);
    assert!(accepts(&profile, &old).await);
    assert!(!accepts(&profile, &new).await);
    assert!(PreparedVmessInboundUsers::from_config_users([
        reference(new_id),
        reference("invalid-second-user"),
    ])
    .is_err());
    assert!(runtime
        .replace_inbound_profile("staged", &[reference(new_id), reference("invalid")])
        .is_err());
    assert!(accepts(&profile, &old).await);
    assert!(!accepts(&profile, &new).await);
}
#[tokio::test]
async fn activation_updates_the_existing_profile_and_preserves_auth_replay_across_aba() {
    let runtime = VmessTransportRuntime::default();
    let old_id = "49494949-4949-4949-4949-494949494949";
    let new_id = "4a4a4a4a-4a4a-4a4a-4a4a-4a4a4a4a4a4a";
    let old = user(0x49);
    let new = user(0x4a);
    let profile = runtime
        .replace_inbound_profile("staged", &[reference(old_id)])
        .unwrap();
    let used = request(&old).await;
    profile
        .accept_request_stream(VmessInbound, Socket::new(used.clone()))
        .await
        .unwrap();
    let activated = stage(&runtime, &[reference(new_id)]).activate().unwrap();
    assert!(!accepts(&profile, &old).await);
    let (facts, _) = profile
        .accept_request_stream(VmessInbound, Socket::new(request(&new).await))
        .await
        .unwrap();
    assert_eq!(facts.auth.principal_key.as_deref(), Some("staged:vmess"));
    assert_eq!(
        (facts.auth.up_bps, facts.auth.down_bps),
        (Some(1024), Some(2048))
    );
    assert_eq!(facts.auth.device_limit, Some(2));
    assert_eq!(facts.auth.quota_remaining_bytes, Some(4096));
    assert_eq!(facts.auth.policy_revision, Some(9));
    stage(&runtime, &[reference(old_id)]).activate().unwrap();
    assert!(
        activated
            .accept_request_stream(VmessInbound, Socket::new(used))
            .await
            .is_err(),
        "the original AuthID remains consumed after A -> B -> A"
    );
    assert!(accepts(&activated, &old).await);
    assert!(!accepts(&activated, &new).await);
    // The existing hot API must still publish immediately to those same handles.
    runtime.replace_inbound_profile("staged", &[]).unwrap();
    assert_eq!(profile.user_count(), 0);
    assert_eq!(activated.user_count(), 0);
}
#[test]
fn pending_profile_does_not_keep_the_runtime_owner_alive() {
    let runtime = VmessTransportRuntime::default();
    let pending = stage(
        &runtime,
        &[reference("4b4b4b4b-4b4b-4b4b-4b4b-4b4b4b4b4b4b")],
    );
    drop(runtime);
    assert!(pending.activate().is_err());
}
