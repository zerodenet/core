#![cfg(feature = "socks5")]
use super::FirstHopHealth;
use std::sync::Arc;
use zero_config::RuntimeConfig;
use zero_engine::Engine;

#[test]
fn retained_prefix_health_does_not_keep_a_retired_configuration_alive() {
    let config = RuntimeConfig::parse(r#"{"outbounds":[{"tag":"hop","protocol":{"type":"socks5","server":"127.0.0.1","port":9}}],"route":{"final":{"type":"route","outbound":"hop"}}}"#).unwrap();
    let engine = Engine::new(config).unwrap();
    let old = engine.runtime_snapshot();
    let retired = Arc::downgrade(old.config());
    let observer = FirstHopHealth::new(&engine, &old, "hop");
    engine
        .reload_runtime_config(old.config().as_ref().clone())
        .unwrap();
    drop(old);
    assert!(
        retired.upgrade().is_none(),
        "prefix observation retained old config"
    );
    drop(observer);
}
