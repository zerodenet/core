//! Published runtime policy, including callers retaining an Engine clone.
use super::*;
use std::time::Duration;

fn config(seconds: u64) -> RuntimeConfig {
    let mut config = config_with_outbounds(1);
    config.runtime.udp_upstream_idle_timeout_seconds = seconds;
    config
}

#[test]
fn published_idle_policy_reaches_existing_engine_clones_and_runtime_exports() {
    let engine = Engine::new(config(60)).unwrap();
    let caller = engine.clone();
    let old = engine.runtime_snapshot();
    engine.reload_runtime_config(config(7)).unwrap();
    assert_eq!(caller.udp_upstream_idle_timeout(), Duration::from_secs(7));
    let status = caller.export_status();
    assert_eq!(status.runtime.config_revision, 2);
    assert_eq!(status.runtime.udp_upstream_idle_timeout_seconds, 7);
    assert_eq!(old.config().runtime.udp_upstream_idle_timeout_seconds, 60);

    let mut invalid = config(3);
    invalid.runtime.udp_upstream_idle_timeout_seconds = 0;
    assert!(engine.reload_runtime_config(invalid).is_err());
    assert_eq!(caller.udp_upstream_idle_timeout(), Duration::from_secs(7));
    assert_eq!(caller.config_revision(), 2);
}

#[test]
fn restoring_an_embedded_stage_restores_the_same_idle_policy() {
    let engine = Engine::new(config(60)).unwrap();
    let old = engine.runtime_snapshot();
    engine.stage_runtime_config(config(7)).unwrap();
    assert_eq!(engine.udp_upstream_idle_timeout(), Duration::from_secs(7));
    engine.restore_staged_snapshot(old, false).unwrap();
    assert_eq!(engine.udp_upstream_idle_timeout(), Duration::from_secs(60));
    assert_eq!(
        engine.export_runtime().udp_upstream_idle_timeout_seconds,
        60
    );
}

#[test]
fn explicit_library_idle_override_survives_configuration_publication() {
    let engine = Engine::new(config(60)).unwrap();
    let overridden = engine
        .clone()
        .with_udp_upstream_idle_timeout(Duration::from_millis(123));
    engine.reload_runtime_config(config(7)).unwrap();
    assert_eq!(engine.udp_upstream_idle_timeout(), Duration::from_secs(7));
    assert_eq!(
        overridden.udp_upstream_idle_timeout(),
        Duration::from_millis(123)
    );
}

#[test]
fn an_unpublished_or_stale_candidate_cannot_change_the_live_idle_policy() {
    let engine = Engine::new(config(60)).unwrap();
    let prepared = zero_runtime::configuration::PreparedConfiguration::prepare_owned(
        config(3),
        engine.configuration_source_owner(),
    )
    .unwrap();
    let stale = engine.prepare_configuration_snapshot(prepared).unwrap();
    assert_eq!(engine.udp_upstream_idle_timeout(), Duration::from_secs(60));
    engine.reload_runtime_config(config(7)).unwrap();
    assert!(engine.publish_prepared_snapshot(stale).is_err());
    assert_eq!(engine.udp_upstream_idle_timeout(), Duration::from_secs(7));
    assert_eq!(
        engine
            .export_status()
            .runtime
            .udp_upstream_idle_timeout_seconds,
        7
    );
}
