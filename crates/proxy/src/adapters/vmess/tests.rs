//! Cold profile updates commit only at the actual host installation point.
use super::*;
use crate::runtime::Proxy;
use tokio::time::{Duration, Instant};

fn candidate() -> (tempfile::TempDir, zero_config::RuntimeConfig) {
    let directory = tempfile::tempdir().unwrap();
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert = directory.path().join("cert.pem");
    let key = directory.path().join("key.pem");
    std::fs::write(&cert, certified.cert.pem()).unwrap();
    std::fs::write(&key, certified.signing_key.serialize_pem()).unwrap();
    let config = zero_config::RuntimeConfig::parse(
        &serde_json::json!({
            "inbounds":[{"tag":"vmess-staged", "listen":{"address":"127.0.0.1","port":0},
                "protocol":{"type":"vmess","users":[
                    {"id":"51515151-5151-5151-5151-515151515151","cipher":"aes-128-gcm"},
                    {"id":"52525252-5252-5252-5252-525252525252","cipher":"aes-128-gcm"}
                ],"tls":{"cert_path":cert,"key_path":key}}}],
            "route":{"final":{"type":"direct"}}
        })
        .to_string(),
    )
    .unwrap();
    (directory, config)
}
#[tokio::test]
async fn candidate_prepare_drop_and_tls_failure_never_publish_live_users_but_install_does() {
    let (directory, config) = candidate();
    let adapter = VmessAdapter::default();
    let InboundProtocolConfig::Vmess { users, .. } = &config.inbounds[0].protocol else {
        unreachable!()
    };
    let old = adapter
        .runtime
        .replace_inbound_profile("vmess-staged", &inbound_user_refs(&users[..1]))
        .unwrap();
    let pending = adapter
        .prepare_inbound_listener(config.inbounds[0].clone(), None)
        .unwrap();
    assert_eq!(old.user_count(), 1);
    drop(pending);
    assert_eq!(old.user_count(), 1);
    let mut invalid = config.inbounds[0].clone();
    let InboundProtocolConfig::Vmess { tls, .. } = &mut invalid.protocol else {
        unreachable!()
    };
    tls.as_mut().unwrap().key_path = "does-not-exist.key".into();
    assert!(adapter.prepare_inbound_listener(invalid, None).is_err());
    assert_eq!(old.user_count(), 1);
    let mut pending = adapter
        .prepare_inbound_listener(config.inbounds[0].clone(), None)
        .unwrap();
    assert_eq!(old.user_count(), 1);
    // All transport material has already been prepared; activation does not
    // reopen either file or silently redo TLS policy at the commit point.
    std::fs::remove_file(directory.path().join("cert.pem")).unwrap();
    std::fs::remove_file(directory.path().join("key.pem")).unwrap();
    let host = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let (stop, _) = tokio::sync::watch::channel(false);
    let installed = host
        .inbound_owners
        .install_listener(
            "vmess-staged",
            &host.protocols,
            pending.internal_tasks(),
            stop,
        )
        .unwrap();
    pending.install_resources(&installed).unwrap();
    assert_eq!(old.user_count(), 2);
    // Idempotent installation must not overwrite a subsequent real hot update.
    adapter
        .runtime
        .replace_inbound_profile("vmess-staged", &[])
        .unwrap();
    pending.install_resources(&installed).unwrap();
    assert_eq!(old.user_count(), 0);
    installed.owner().seal_listener_installation();
    host.inbound_owners
        .shutdown("vmess-staged", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}
#[test]
fn post_reconcile_hot_callback_still_publishes_to_the_existing_actual_profile() {
    let (_directory, mut config) = candidate();
    let adapter = VmessAdapter::default();
    let InboundProtocolConfig::Vmess { users, .. } = &config.inbounds[0].protocol else {
        unreachable!()
    };
    let profile = adapter
        .runtime
        .replace_inbound_profile("vmess-staged", &inbound_user_refs(&users[..1]))
        .unwrap();
    adapter.on_config_reloaded(&config);
    assert_eq!(profile.user_count(), 2);
    let InboundProtocolConfig::Vmess { users, .. } = &mut config.inbounds[0].protocol else {
        unreachable!()
    };
    users.clear();
    adapter.on_config_reloaded(&config);
    assert_eq!(profile.user_count(), 0);
}
