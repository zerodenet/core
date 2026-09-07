use serde_json::{json, Value};
use zero_config::RuntimeConfig;
use zero_engine::{Engine, UrlTestMemberState};

fn config(nodes: &[&str], groups: Value) -> RuntimeConfig {
    RuntimeConfig::parse(
        &json!({
            "outbounds": nodes.iter().map(|tag| json!({
                "tag": tag, "protocol": {"type": "direct"}
            })).collect::<Vec<_>>(),
            "outbound_groups": groups,
            "route": {"rules": [], "final": {"type": "direct"}}
        })
        .to_string(),
    )
    .unwrap()
}

fn selector(members: &[&str], selected: &str) -> Value {
    json!({"tag": "select", "type": "selector", "outbounds": members, "selected": selected})
}

fn auto() -> Value {
    json!({"tag": "auto", "type": "url_test", "outbounds": ["a", "b"],
        "url": "http://example.com", "interval_seconds": 60})
}

#[test]
fn removing_a_selected_group_keeps_status_and_routing_valid() {
    let engine = Engine::new(config(
        &["a"],
        json!([
            selector(&["later", "a"], "later"),
            {"tag": "later", "type": "fallback", "outbounds": ["a"]}
        ]),
    ))
    .unwrap();
    let old = engine.runtime_snapshot();
    engine
        .reload_runtime_config(config(&["a"], json!([selector(&["a"], "a")])))
        .unwrap();
    let status = engine.export_status();
    assert_eq!(
        status.config.outbound_groups[0].selected.as_deref(),
        Some("a")
    );
    let new = engine.runtime_snapshot();
    for snapshot in [&old, &new] {
        let id = snapshot.plan().target_id("select").unwrap();
        assert!(engine.resolve_target_id_in_snapshot(snapshot, id).is_some());
    }
    assert_eq!(
        engine.resolve_target_chains_in_snapshot(&old, old.plan().target_id("select").unwrap())[0]
            .len(),
        3
    );
}

#[test]
fn reordering_nodes_remaps_selection_by_tag_and_isolates_old_snapshots() {
    let groups = json!([selector(&["a", "b"], "a")]);
    let engine = Engine::new(config(&["a", "b"], groups.clone())).unwrap();
    engine.set_selector_target("select", "b").unwrap();
    let old = engine.runtime_snapshot();
    engine
        .reload_runtime_config(config(&["b", "a"], groups))
        .unwrap();
    assert_eq!(
        engine.export_status().config.outbound_groups[0]
            .selected
            .as_deref(),
        Some("b")
    );
    engine.set_selector_target("select", "a").unwrap();
    let chains =
        engine.resolve_target_chains_in_snapshot(&old, old.plan().target_id("select").unwrap());
    assert_eq!(
        old.plan().target(*chains[0].last().unwrap()).unwrap().tag(),
        "b"
    );
}

#[test]
fn reordering_cannot_select_a_node_outside_the_group() {
    let groups = json!([selector(&["a"], "a")]);
    let engine = Engine::new(config(&["a", "b"], groups.clone())).unwrap();
    engine
        .reload_runtime_config(config(&["b", "a"], groups))
        .unwrap();
    let status = engine.export_status();
    let policy = &status.config.outbound_groups[0];
    assert_eq!(policy.selected.as_deref(), Some("a"));
    assert_eq!(policy.effective_chains, vec![vec!["select", "a"]]);
}

#[test]
fn changed_selection_and_removed_runtime_member_use_the_new_configuration() {
    let engine = Engine::new(config(&["a", "b"], json!([selector(&["a", "b"], "a")]))).unwrap();
    engine
        .reload_runtime_config(config(&["a", "b"], json!([selector(&["a", "b"], "b")])))
        .unwrap();
    assert_eq!(
        engine.export_status().config.outbound_groups[0]
            .selected
            .as_deref(),
        Some("b")
    );
    engine
        .reload_runtime_config(config(&["a", "b"], json!([selector(&["a"], "a")])))
        .unwrap();
    assert_eq!(
        engine.export_status().config.outbound_groups[0]
            .selected
            .as_deref(),
        Some("a")
    );
}

#[test]
fn late_urltest_results_and_historical_chains_cannot_pollute_a_new_plan() {
    let engine = Engine::new(config(&["a", "b"], json!([auto()]))).unwrap();
    let old = engine.runtime_snapshot();
    let group = old.plan().target_id("auto").unwrap();
    let member = old.plan().target_id("b").unwrap();
    let publish_old_result = || {
        let selection = old
            .plan()
            .target(group)
            .unwrap()
            .as_urltest()
            .unwrap()
            .select(None, &[(member, 10)]);
        old.update_urltest_state(
            group,
            member,
            Some(10),
            vec![UrlTestMemberState {
                member_id: member,
                healthy: true,
                latency_ms: Some(10),
                last_checked_unix_ms: Some(1),
                last_error: None,
                effective_chains: vec![vec![group, member]],
            }],
            selection,
        );
    };
    publish_old_result();
    engine
        .stage_runtime_config(config(&["b", "a", "c"], json!([auto()])))
        .unwrap();
    let staged = engine.runtime_snapshot();
    // Staging retains the committed revision, so revision equality cannot be
    // used as a substitute for generation ownership.
    assert_eq!(old.config_revision(), staged.config_revision());
    publish_old_result();
    let policy = engine.export_status().config.outbound_groups.remove(0);
    assert_eq!(policy.selected.as_deref(), Some("b"));
    let selection = policy.url_test_selection.as_ref().unwrap();
    assert_eq!(selection.selected, "b");
    assert_eq!(selection.reason, "initial");
    assert!(selection.previous_selected.is_none());
    assert!(selection.best_candidate.is_none());
    assert!(selection.best_latency_ms.is_none());
    assert!(staged
        .urltest_state(staged.plan().target_id("auto").unwrap())
        .unwrap()
        .selection
        .is_none());
    assert!(policy
        .url_test_members
        .iter()
        .all(|member| !member.healthy && member.effective_chains.is_empty()));
    assert!(old.urltest_state(group).unwrap().members[0].healthy);
    engine.commit_config_change();
    publish_old_result();
    assert_eq!(engine.export_status().config.outbound_groups[0], policy);
}

#[test]
fn removing_a_urltest_member_discards_its_selection_history_and_chain_ids() {
    let mut old_auto = auto();
    old_auto["outbounds"] = json!(["later", "a"]);
    let engine = Engine::new(config(
        &["a", "b"],
        json!([
            old_auto, {"tag": "later", "type": "fallback", "outbounds": ["b"]}
        ]),
    ))
    .unwrap();
    let old = engine.runtime_snapshot();
    let group = old.plan().target_id("auto").unwrap();
    let later = old.plan().target_id("later").unwrap();
    let b = old.plan().target_id("b").unwrap();
    let selection = old
        .plan()
        .target(group)
        .unwrap()
        .as_urltest()
        .unwrap()
        .select(Some(later), &[(later, 10)]);
    old.update_urltest_state(
        group,
        later,
        Some(10),
        vec![UrlTestMemberState {
            member_id: later,
            healthy: true,
            latency_ms: Some(10),
            last_checked_unix_ms: Some(1),
            last_error: None,
            effective_chains: vec![vec![group, later, b]],
        }],
        selection,
    );
    engine
        .reload_runtime_config(config(&["a", "b"], json!([auto()])))
        .unwrap();
    let policy = engine.export_status().config.outbound_groups.remove(0);
    assert_eq!(policy.selected.as_deref(), Some("a"));
    assert_eq!(policy.effective_chains, vec![vec!["auto", "a"]]);
    assert!(policy
        .url_test_selection
        .unwrap()
        .previous_selected
        .is_none());
    assert!(policy
        .url_test_members
        .iter()
        .all(|member| member.effective_chains.is_empty()));
    assert!(engine.resolve_target_id_in_snapshot(&old, group).is_some());
}

#[test]
fn rejected_staging_restores_the_original_policy_state_and_snapshot() {
    let engine = Engine::new(config(&["a", "b"], json!([selector(&["a", "b"], "a")]))).unwrap();
    engine.set_selector_target("select", "b").unwrap();
    let original = engine.runtime_snapshot();
    engine
        .stage_runtime_config(config(&["a"], json!([selector(&["a"], "a")])))
        .unwrap();
    let rejected = engine.runtime_snapshot();
    engine
        .restore_staged_snapshot(original.clone(), false)
        .unwrap();
    assert!(std::sync::Arc::ptr_eq(
        &original,
        &engine.runtime_snapshot()
    ));
    assert_eq!(
        engine.export_status().config.outbound_groups[0]
            .selected
            .as_deref(),
        Some("b")
    );
    assert_eq!(engine.config_revision(), 1);
    assert!(engine
        .resolve_target_id_in_snapshot(&rejected, rejected.plan().target_id("select").unwrap())
        .is_some());
    engine.commit_config_change();
    assert!(engine.restore_staged_snapshot(rejected, false).is_err());
}
