use super::*;
use crate::transport::ShadowsocksTransportState;
fn input<'a>(cipher: &'a str, password: &'a str) -> TcpPreparation<'a> {
    TcpPreparation {
        server: "127.0.0.1",
        port: 443,
        protocol: ShadowsocksOutboundOptionsRef { cipher, password },
        replay_policy: ReplayPolicy::Reject,
        limits: StateLimits {
            tcp_replay_capacity: Some(3),
            ..Default::default()
        },
        plugin: None,
        dial: Default::default(),
    }
}
#[test]
fn prepared_tcp_reuses_the_adapter_resource_history_without_a_fresh_pool_or_implicit_binding() {
    let state = ShadowsocksTransportState::default();
    let prepared = state
        .prepare_tcp(input("2022-blake3-aes-128-gcm", "AAAAAAAAAAAAAAAAAAAAAA=="))
        .unwrap();
    let existing = state.replay_component_options(ReplayPolicy::Reject, Some(3));
    assert!(Arc::ptr_eq(
        &prepared.replay_options().unwrap().pool().unwrap(),
        &existing.pool().unwrap()
    ));
    assert!(
        prepared.bind(None).is_err(),
        "2022 cannot silently omit the actual shared owner"
    );
    let legacy = state
        .prepare_tcp(input("aes-128-gcm", "secret-not-in-debug"))
        .unwrap();
    assert!(legacy.replay_options().is_none());
    assert!(!format!("{legacy:?}").contains("secret-not-in-debug"));
    assert!(legacy.bind(None).is_ok());
}
#[test]
fn configured_plugins_and_invalid_protocol_values_fail_during_cold_preparation() {
    let state = ShadowsocksTransportState::default();
    let plugin = PluginConfig {
        command: "not-spawned".into(),
        args: vec![],
        options: None,
        mode: crate::validation::PluginMode::TcpOnly,
    };
    let mut configured = input("aes-128-gcm", "password");
    configured.plugin = Some(&plugin);
    assert!(matches!(
        state.prepare_tcp(configured),
        Err(Error::Unsupported(_))
    ));
    assert!(state
        .prepare_tcp(input("2022-blake3-aes-128-gcm", "invalid-key"))
        .is_err());
    assert!(state
        .prepare_tcp(input("unknown-cipher", "password"))
        .is_err());
}
