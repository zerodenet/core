use std::sync::Arc;

use zero_config::RuntimeConfig;
use zero_engine::{Engine, ResolvedLeafOutbound, ResolvedOutbound};

#[test]
fn reload_publishes_plan_and_config_as_one_generation() {
    let engine = Engine::new(config_with_outbounds(3)).expect("build engine");
    let old = engine.runtime_snapshot();

    engine
        .reload_runtime_config(config_with_outbounds(35))
        .expect("reload larger config");
    let new = engine.runtime_snapshot();

    assert!(!Arc::ptr_eq(&old, &new));
    assert_eq!(old.config().outbounds.len(), 3);
    assert_eq!(new.config().outbounds.len(), 35);
    assert_snapshot_resolves_tag(&engine, &old, "node-2", 2);
    assert_snapshot_resolves_tag(&engine, &new, "node-34", 34);
    assert!(old.plan().target_id("node-34").is_none());
}

#[test]
fn old_snapshot_survives_large_to_small_reload() {
    let engine = Engine::new(config_with_outbounds(35)).expect("build engine");
    let old = engine.runtime_snapshot();

    engine
        .reload_runtime_config(config_with_outbounds(3))
        .expect("reload smaller config");
    let new = engine.runtime_snapshot();

    assert_snapshot_resolves_tag(&engine, &old, "node-34", 34);
    assert_snapshot_resolves_tag(&engine, &new, "node-2", 2);
    assert!(new.plan().target_id("node-34").is_none());
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
