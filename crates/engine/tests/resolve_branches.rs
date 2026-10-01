use serde_json::{json, Value};
use zero_config::RuntimeConfig;
use zero_core::Address;
use zero_engine::{Engine, ResolvedLeafOutbound, ResolvedOutbound, RouteDecision};

fn engine(groups: Vec<Value>, route: &str) -> Engine {
    let config: RuntimeConfig = serde_json::from_value(json!({
        "outbounds": [
            {"tag": "primary", "protocol": {"type": "socks5",
                "server": "127.0.0.1", "port": 1080}},
            {"tag": "alternate", "protocol": {"type": "socks5",
                "server": "127.0.0.1", "port": 1081}}
        ],
        "outbound_groups": groups,
        "route": {"rules": [], "final": {"type": "route", "outbound": route}}
    }))
    .expect("parse config");
    config.validate().expect("configuration is accepted");
    Engine::new(config).expect("build engine")
}

fn unresolved_relay() -> Vec<Value> {
    vec![
        json!({"tag": "choices", "type": "fallback",
            "outbounds": ["primary", "alternate"]}),
        json!({"tag": "chain", "type": "relay",
            "proxies": ["choices", "alternate"]}),
    ]
}

fn candidate_indices(engine: &Engine, tag: &str) -> Vec<usize> {
    let (resolved, _, _) = engine
        .resolve_route_decision_for_flow(
            RouteDecision::Route(tag.to_owned()),
            &Address::Domain("application.example".to_owned()),
            443,
        )
        .expect("resolve remaining configured candidates");
    let ResolvedOutbound::Fallback { candidates } = resolved else {
        panic!("expected fallback candidates");
    };
    candidates
        .into_iter()
        .map(|candidate| match candidate {
            ResolvedLeafOutbound::Proxy { identity } => identity.config_index(),
            _ => panic!("no unconfigured direct or block candidate"),
        })
        .collect()
}

#[test]
fn fallback_continues_after_a_relay_hop_cannot_resolve_to_one_leaf() {
    let mut groups = unresolved_relay();
    groups.push(json!({"tag": "outer", "type": "fallback",
        "outbounds": ["chain", "primary"]}));
    let engine = engine(groups, "outer");
    assert!(engine
        .resolve_target_id(engine.plan().target_id("chain").unwrap())
        .is_none());
    assert_eq!(candidate_indices(&engine, "outer"), vec![0]);
}

#[test]
fn loadbalance_preserves_rotation_when_a_relay_candidate_cannot_resolve() {
    let mut groups = unresolved_relay();
    groups.push(json!({"tag": "outer", "type": "load_balance",
        "outbounds": ["chain", "primary", "alternate"], "strategy": "round_robin"}));
    let engine = engine(groups, "outer");
    for expected in [vec![0, 1], vec![0, 1], vec![1, 0], vec![0, 1]] {
        assert_eq!(candidate_indices(&engine, "outer"), expected);
    }
}

#[test]
fn failed_branch_does_not_leave_a_shared_group_on_the_recursion_stack() {
    let mut groups = unresolved_relay();
    groups.extend([
        json!({"tag": "inner", "type": "fallback",
            "outbounds": ["chain", "primary"]}),
        json!({"tag": "wrapper", "type": "selector", "outbounds": ["inner"]}),
        json!({"tag": "outer", "type": "fallback",
            "outbounds": ["inner", "wrapper"]}),
    ]);
    let engine = engine(groups, "outer");
    assert_eq!(candidate_indices(&engine, "outer"), vec![0, 0]);
}

#[test]
fn groups_with_no_executable_candidates_fail_resolution() {
    let mut groups = unresolved_relay();
    groups.extend([
        json!({"tag": "empty_fallback", "type": "fallback", "outbounds": ["chain"]}),
        json!({"tag": "empty_balance", "type": "load_balance",
            "outbounds": ["chain"], "strategy": "round_robin"}),
    ]);
    let engine = engine(groups, "empty_fallback");
    for tag in ["empty_fallback", "empty_balance"] {
        assert!(engine
            .resolve_target_id(engine.plan().target_id(tag).unwrap())
            .is_none());
    }
}
