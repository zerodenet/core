use serde_json::{Value, json};
use zero_config::RuntimeConfig;
use zero_engine::{
    Engine, EngineRuntimeSnapshot, ResolvedLeafOutbound, ResolvedOutbound, RouteDecision,
};
use zero_traits::{AddressFamily, DialPolicy};

fn config(family: &str) -> RuntimeConfig {
    RuntimeConfig::parse(
        &json!({
            "outbounds": [
                {"tag": "direct", "protocol": {"type": "direct"},
                    "dial": {"address_family": family, "interface": "Ethernet 2"}},
                {"tag": "other", "protocol": {"type": "direct"},
                    "dial": {"source_ip": "::ffff:127.0.0.1"}},
                {"tag": "block", "protocol": {"type": "block"}}
            ],
            "outbound_groups": [
                {"tag": "pick", "type": "selector", "outbounds": ["direct", "other"]},
                {"tag": "backup", "type": "fallback", "outbounds": ["direct", "other"]},
                {"tag": "balanced", "type": "load_balance", "outbounds": ["direct", "other"],
                    "strategy": "round_robin"}
            ],
            "route": {"rules": [], "final": {"type": "direct"}}
        })
        .to_string(),
    )
    .unwrap()
}

fn policy(engine: &Engine, tag: Option<&str>) -> (DialPolicy, u64) {
    engine.direct_dial_policy(tag).unwrap()
}

fn assert_direct_leaf(leaf: ResolvedLeafOutbound<'_>, snapshot: &EngineRuntimeSnapshot) -> String {
    let ResolvedLeafOutbound::Direct {
        tag,
        dial_policy,
        dial_generation,
    } = leaf
    else {
        panic!("expected Direct leaf");
    };
    assert_eq!(
        snapshot.direct_dial_policy(tag),
        Some((dial_policy, dial_generation))
    );
    assert_ne!(dial_generation, 0);
    tag.unwrap().to_owned()
}

#[test]
fn direct_leaf_and_group_expansion_preserve_each_tags_policy_identity() {
    let engine = Engine::new(config("only_ipv6")).unwrap();
    let snapshot = engine.runtime_snapshot();
    for tag in ["direct", "other", "pick"] {
        let id = snapshot.plan().target_id(tag).unwrap();
        let (ResolvedOutbound::Single(leaf), _plan) =
            engine.resolve_target_id_in_snapshot(&snapshot, id).unwrap()
        else {
            panic!("expected one selected leaf");
        };
        let expected = if tag == "pick" { "direct" } else { tag };
        assert_eq!(assert_direct_leaf(leaf, &snapshot), expected);
    }
    for tag in ["backup", "balanced"] {
        let id = snapshot.plan().target_id(tag).unwrap();
        let (ResolvedOutbound::Fallback { candidates }, _plan) =
            engine.resolve_target_id_in_snapshot(&snapshot, id).unwrap()
        else {
            panic!("expected separate Direct candidates");
        };
        assert_eq!(
            candidates
                .into_iter()
                .map(|leaf| assert_direct_leaf(leaf, &snapshot))
                .collect::<Vec<_>>(),
            ["direct", "other"]
        );
    }
    assert_eq!(
        policy(&engine, Some("direct")).0.address_family,
        AddressFamily::OnlyIpv6
    );
    assert_eq!(
        policy(&engine, Some("other")).0.source_ip,
        Some("127.0.0.1".parse().unwrap())
    );
    assert!(engine.direct_dial_policy(Some("pick")).is_none());
    assert!(engine.direct_dial_policy(Some("block")).is_none());
    assert!(engine.direct_dial_policy(Some("missing")).is_none());
}

#[test]
fn implicit_direct_never_inherits_a_tag_named_direct() {
    let engine = Engine::new(config("only_ipv6")).unwrap();
    let (
        ResolvedOutbound::Single(ResolvedLeafOutbound::Direct {
            tag,
            dial_policy,
            dial_generation,
        }),
        _plan,
    ) = engine
        .resolve_route_decision(RouteDecision::Direct)
        .unwrap()
    else {
        panic!("expected implicit Direct");
    };
    assert_eq!(tag, None);
    assert_eq!(dial_policy, DialPolicy::default());
    assert_eq!(dial_generation, 0);
    assert_eq!(policy(&engine, None), (DialPolicy::default(), 0));
    assert_ne!(policy(&engine, Some("direct")), policy(&engine, None));
}

#[test]
fn unchanged_policy_survives_reload_but_a_b_a_gets_fresh_generation() {
    let engine = Engine::new(config("only_ipv4")).unwrap();
    let old = engine.runtime_snapshot();
    let first = policy(&engine, Some("direct"));
    let other = policy(&engine, Some("other"));
    let mut unrelated = config("only_ipv4");
    unrelated.runtime.event_log_capacity += 1;
    engine.reload_runtime_config(unrelated).unwrap();
    assert_eq!(policy(&engine, Some("direct")), first);
    engine.reload_runtime_config(config("only_ipv6")).unwrap();
    let second = policy(&engine, Some("direct"));
    assert!(second.1 > first.1);
    assert_eq!(policy(&engine, Some("other")), other);
    engine.reload_runtime_config(config("only_ipv4")).unwrap();
    let third = policy(&engine, Some("direct"));
    assert_eq!(third.0, first.0);
    assert!(third.1 > second.1);
    assert_eq!(old.direct_dial_policy(Some("direct")), Some(first));
    assert_eq!(policy(&engine, Some("other")), other);
}

#[test]
fn removed_and_readded_policy_does_not_reuse_the_old_identity() {
    let engine = Engine::new(config("only_ipv4")).unwrap();
    let first = policy(&engine, Some("direct"));
    let mut removed = config("only_ipv4");
    removed.outbounds.clear();
    removed.outbound_groups.clear();
    engine.reload_runtime_config(removed).unwrap();
    assert!(engine.direct_dial_policy(Some("direct")).is_none());
    engine.reload_runtime_config(config("only_ipv4")).unwrap();
    let readded = policy(&engine, Some("direct"));
    assert_eq!(readded.0, first.0);
    assert!(readded.1 > first.1);
}

#[test]
fn rejected_stage_restores_old_identity_without_reusing_staged_generation() {
    let engine = Engine::new(config("only_ipv4")).unwrap();
    let old = engine.runtime_snapshot();
    let first = policy(&engine, Some("direct"));
    engine.stage_runtime_config(config("only_ipv6")).unwrap();
    let staged = policy(&engine, Some("direct"));
    engine.restore_staged_snapshot(old, false).unwrap();
    assert_eq!(policy(&engine, Some("direct")), first);
    engine.stage_runtime_config(config("only_ipv6")).unwrap();
    let restaged = policy(&engine, Some("direct"));
    assert_eq!(restaged.0, staged.0);
    assert!(restaged.1 > staged.1);
}

#[test]
fn serialized_reload_keeps_dial_fields_and_mapped_source_canonicalization() {
    let engine = Engine::new(config("only_ipv6")).unwrap();
    let serialized = serde_json::to_value(&*engine.config()).unwrap();
    assert_eq!(
        serialized["outbounds"][0]["dial"]["address_family"],
        "only_ipv6"
    );
    assert_eq!(serialized["outbounds"][1]["dial"]["source_ip"], "127.0.0.1");
    assert_eq!(serialized["outbounds"][2].get("dial"), None::<&Value>);
    let parsed = RuntimeConfig::parse(&serialized.to_string()).unwrap();
    let before = policy(&engine, Some("direct"));
    engine.reload_runtime_config(parsed).unwrap();
    assert_eq!(policy(&engine, Some("direct")), before);
}
