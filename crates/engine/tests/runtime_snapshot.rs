use std::sync::Arc;

use zero_api::{HealthQuery, QueryRequest, QueryResponse, QueryService};
use zero_config::RuntimeConfig;
use zero_engine::{Engine, ResolvedLeafOutbound, ResolvedOutbound};

#[test]
fn reload_publishes_plan_and_config_as_one_generation() {
    let engine = Engine::new(config_with_outbounds(3)).expect("build engine");
    let old = engine.runtime_snapshot();
    assert_eq!(old.config_revision(), 1);

    engine
        .reload_runtime_config(config_with_outbounds(35))
        .expect("reload larger config");
    let new = engine.runtime_snapshot();

    assert!(!Arc::ptr_eq(&old, &new));
    assert_eq!(new.config_revision(), 2);
    assert_eq!(engine.config_revision(), 2);
    assert_eq!(old.config_revision(), 1);
    assert_eq!(old.config().outbounds.len(), 3);
    assert_eq!(new.config().outbounds.len(), 35);
    assert_snapshot_resolves_tag(&engine, &old, "node-2", 2);
    assert_snapshot_resolves_tag(&engine, &new, "node-34", 34);
    assert!(old.plan().target_id("node-34").is_none());

    let status = engine.export_status();
    assert_eq!(status.config.config_revision, 2);
    assert_eq!(status.runtime.config_revision, 2);
    assert_eq!(status.runtime.core_instance_id, engine.core_instance_id());
    let QueryResponse::Health(health) = engine
        .query(QueryRequest::Health(HealthQuery))
        .expect("query health")
    else {
        panic!("expected health response");
    };
    assert_eq!(health.core_instance_id, engine.core_instance_id());
    assert_eq!(health.config_revision, 2);
}

#[test]
fn old_snapshot_survives_large_to_small_reload() {
    let engine = Engine::new(config_with_outbounds(35)).expect("build engine");
    let old = engine.runtime_snapshot();
    assert_eq!(old.config_revision(), 1);

    engine
        .reload_runtime_config(config_with_outbounds(3))
        .expect("reload smaller config");
    let new = engine.runtime_snapshot();
    assert_eq!(new.config_revision(), 2);

    assert_snapshot_resolves_tag(&engine, &old, "node-34", 34);
    assert_snapshot_resolves_tag(&engine, &new, "node-2", 2);
    assert!(new.plan().target_id("node-34").is_none());
}

#[test]
fn staged_snapshot_is_promoted_in_place_only_when_committed() {
    let engine = Engine::new(config_with_outbounds(3)).expect("build engine");
    let original = engine.runtime_snapshot();
    engine
        .stage_runtime_config(config_with_outbounds(4))
        .expect("stage candidate");
    let staged = engine.runtime_snapshot();

    assert_eq!(original.config_revision(), 1);
    assert_eq!(staged.config_revision(), 1);
    assert_eq!(staged.config().outbounds.len(), 4);

    assert_eq!(engine.commit_config_change(), 2);
    assert_eq!(staged.config_revision(), 2);
    assert_eq!(original.config_revision(), 1);
}

fn assert_snapshot_resolves_tag(
    engine: &Engine,
    snapshot: &zero_engine::EngineRuntimeSnapshot,
    tag: &str,
    expected_index: usize,
) {
    let target_id = snapshot.plan().target_id(tag).expect("target id");
    let (resolved, _plan) = engine
        .resolve_target_id_in_snapshot(snapshot, target_id)
        .expect("resolve target in snapshot");
    let ResolvedOutbound::Single(ResolvedLeafOutbound::Proxy { identity }) = resolved else {
        panic!("expected proxy leaf");
    };
    assert_eq!(identity.config_index(), expected_index);
    assert_eq!(snapshot.config().outbounds[expected_index].tag, tag);
}

fn config_with_outbounds(count: usize) -> RuntimeConfig {
    let outbounds = (0..count)
        .map(|index| {
            serde_json::json!({
                "tag": format!("node-{index}"),
                "protocol": {
                    "type": "socks5",
                    "server": format!("node-{index}.example"),
                    "port": 1080
                }
            })
        })
        .collect::<Vec<_>>();
    RuntimeConfig::parse(
        &serde_json::json!({
            "inbounds": [],
            "outbounds": outbounds,
            "route": { "rules": [], "final": { "type": "direct" } }
        })
        .to_string(),
    )
    .expect("parse generated config")
}
