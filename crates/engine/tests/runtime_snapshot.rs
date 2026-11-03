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

#[test]
fn engine_only_persistent_stage_and_restore_keep_their_existing_source_scope() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("zero.json");
    let original = config_with_outbounds(1);
    std::fs::write(&path, serde_json::to_vec_pretty(&original).unwrap()).unwrap();
    let engine = Engine::new_with_config_path(original, &path).unwrap();
    let previous = engine.runtime_snapshot();
    let revision = engine.config_revision();
    engine.stage_config(config_with_outbounds(2)).unwrap();
    assert_eq!(
        RuntimeConfig::load_from_path(&path)
            .unwrap()
            .outbounds
            .len(),
        2
    );
    assert_eq!(engine.config_revision(), revision);
    engine
        .restore_staged_snapshot(previous.clone(), true)
        .unwrap();
    assert!(std::sync::Arc::ptr_eq(
        &engine.runtime_snapshot(),
        &previous
    ));
    assert_eq!(
        RuntimeConfig::load_from_path(&path)
            .unwrap()
            .outbounds
            .len(),
        1
    );
    engine
        .stage_runtime_config(config_with_outbounds(3))
        .unwrap();
    assert_eq!(
        RuntimeConfig::load_from_path(&path)
            .unwrap()
            .outbounds
            .len(),
        1
    );
    engine.restore_staged_snapshot(previous, false).unwrap();
    engine.reload_config(config_with_outbounds(2)).unwrap();
    assert_eq!(engine.config_revision(), revision + 1);
    assert!(!directory.path().join("zero.json.zero-recovery").exists());
    assert!(!directory.path().join("zero.json.zero-last-good").exists());
    // A normal operator edit is accepted after the actual source commit ends.
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&config_with_outbounds(4)).unwrap(),
    )
    .unwrap();
    let restarted = Engine::from_path(&path).unwrap();
    assert_eq!(restarted.config().outbounds.len(), 4);
    assert_ne!(restarted.core_instance_id(), engine.core_instance_id());
}

#[test]
fn engine_only_v1_validate_prepares_relative_resources_without_installing_or_persisting() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("zero.json");
    let original = br#"{"route":{"final":{"type":"direct"}}}"#;
    std::fs::write(&path, original).unwrap();
    let engine = Engine::from_path(&path).unwrap();
    let previous = engine.runtime_snapshot();
    let handle = zero_engine::EngineHandle::new(engine.clone());
    let candidate = serde_json::json!({"route":{
        "rule_sets":[{"tag":"domains","type":"file","path":"domains.txt","format":"domain_list"}],
        "rules":[{"condition":{"type":"rule_set","tag":"domains"},"action":{"type":"reject"}}],
        "final":{"type":"direct"}}});
    let error = zero_api::CommandService::execute(
        &handle,
        zero_api::CommandRequest::ConfigValidate(zero_api::ConfigValidateCommand {
            config: candidate.clone(),
        }),
    )
    .unwrap_err();
    assert!(error.message.contains("domains.txt"));
    assert!(std::sync::Arc::ptr_eq(
        &engine.runtime_snapshot(),
        &previous
    ));
    assert_eq!(std::fs::read(&path).unwrap(), original);
    std::fs::write(directory.path().join("domains.txt"), b"example.test\n").unwrap();
    let accepted = zero_api::CommandService::execute(
        &handle,
        zero_api::CommandRequest::ConfigValidate(zero_api::ConfigValidateCommand {
            config: candidate.clone(),
        }),
    )
    .unwrap();
    assert!(accepted.accepted);
    assert!(std::sync::Arc::ptr_eq(
        &engine.runtime_snapshot(),
        &previous
    ));
    assert_eq!(engine.config_source_write_generation(), 0);
    std::fs::remove_file(directory.path().join("domains.txt")).unwrap();
    assert!(zero_api::CommandService::execute(
        &handle,
        zero_api::CommandRequest::ConfigApply(zero_api::ConfigApplyCommand { config: candidate })
    )
    .is_err());
    assert!(std::sync::Arc::ptr_eq(
        &engine.runtime_snapshot(),
        &previous
    ));
    assert_eq!(std::fs::read(path).unwrap(), original);
}
