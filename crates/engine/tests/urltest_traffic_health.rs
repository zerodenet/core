use serde_json::{json, Value};
use zero_config::RuntimeConfig;
use zero_core::Address;
use zero_engine::{Engine, ResolvedLeafOutbound, ResolvedOutbound, RouteDecision};

#[path = "urltest_traffic_health/immediate.rs"]
mod immediate;

fn engine(members: &[&str], extra_groups: Vec<Value>) -> Engine {
    let mut groups = vec![json!({
        "tag": "auto", "type": "url_test", "outbounds": members,
        "url": "http://probe.example/", "interval_seconds": 300
    })];
    groups.extend(extra_groups);
    let config: RuntimeConfig = serde_json::from_value(json!({
        "outbounds": [
            {"tag": "primary", "protocol": {
                "type": "shadowsocks", "server": "primary.example", "port": 443,
                "password": "password", "cipher": "aes-256-gcm"
            }},
            {"tag": "alternate", "protocol": {
                "type": "shadowsocks", "server": "alternate.example", "port": 443,
                "password": "password", "cipher": "aes-256-gcm"
            }},
            {"tag": "direct", "protocol": {"type": "direct"}}
        ],
        "outbound_groups": groups,
        "route": {"rules": [], "final": {"type": "route", "outbound": "auto"}}
    }))
    .expect("parse config");
    Engine::new(config).expect("build engine")
}

fn quarantine(engine: &Engine, tag: &str) {
    for _ in 0..5 {
        engine.record_outbound_failure(tag);
    }
    engine.check_outbound_health(tag).expect_err("quarantined");
}

fn resolve(engine: &Engine, route: &str) -> ResolvedOutbound<'static> {
    engine
        .resolve_route_decision_for_flow(
            RouteDecision::Route(route.to_owned()),
            &Address::Domain("application.example".to_owned()),
            443,
        )
        .expect("resolve route")
        .0
}

fn single_index(resolved: ResolvedOutbound<'_>) -> usize {
    let ResolvedOutbound::Single(ResolvedLeafOutbound::Proxy { identity }) = resolved else {
        panic!("expected one proxy leaf");
    };
    identity.config_index()
}

#[test]
fn quarantined_urltest_member_is_skipped_before_the_next_scheduled_probe() {
    let engine = engine(&["primary", "alternate"], vec![]);
    assert_eq!(single_index(resolve(&engine, "auto")), 0);
    quarantine(&engine, "primary");

    let (resolved, _, selections) = engine
        .resolve_route_decision_for_flow(
            RouteDecision::Route("auto".to_owned()),
            &Address::Domain("application.example".to_owned()),
            443,
        )
        .expect("resolve route");
    assert_eq!(single_index(resolved), 1);
    assert_eq!(selections[0].member_tag, "alternate");
    let snapshot = engine.runtime_snapshot();
    let group = snapshot.plan().target_id("auto").unwrap();
    assert_eq!(
        snapshot.urltest_selected_target(group),
        snapshot.plan().target_id("alternate")
    );
}

#[test]
fn successful_traffic_restores_eligibility_without_forcing_a_switch_back() {
    let engine = engine(&["primary", "alternate"], vec![]);
    quarantine(&engine, "primary");
    assert_eq!(single_index(resolve(&engine, "auto")), 1);
    engine.record_outbound_success("primary");
    engine
        .check_outbound_health("primary")
        .expect("eligible again");
    assert_eq!(single_index(resolve(&engine, "auto")), 1);
}

#[test]
fn urltest_health_checks_follow_a_selected_nested_selector_leaf() {
    let engine = engine(
        &["choice", "alternate"],
        vec![json!({
            "tag": "choice", "type": "selector", "outbounds": ["primary", "direct"]
        })],
    );
    quarantine(&engine, "primary");
    assert_eq!(single_index(resolve(&engine, "auto")), 1);
    // The manually selected leaf stays selected outside URLTest routing.
    assert_eq!(single_index(resolve(&engine, "choice")), 0);
}

#[test]
fn all_cooling_members_keep_selection_and_report_unavailable_until_actual_recovery() {
    let engine = engine(&["primary", "alternate"], vec![]);
    quarantine(&engine, "primary");
    let snapshot = engine.runtime_snapshot();
    let group = snapshot.plan().target_id("auto").unwrap();
    let selected = snapshot.urltest_selected_target(group);
    quarantine(&engine, "alternate");
    assert_eq!(snapshot.urltest_selected_target(group), selected);
    let error = engine
        .resolve_route_decision_for_flow(
            RouteDecision::Route("auto".to_owned()),
            &Address::Domain("application.example".to_owned()),
            443,
        )
        .expect_err("retain main's explicit all-unavailable result, without inventing Direct");
    assert_eq!(error.code(), "no_usable_urltest_member");
    engine.record_outbound_success("primary");
    assert_eq!(single_index(resolve(&engine, "auto")), 0);
}

#[test]
fn explicit_proxy_routes_keep_the_manually_chosen_leaf() {
    let engine = engine(&["primary", "alternate"], vec![]);
    quarantine(&engine, "primary");
    assert_eq!(single_index(resolve(&engine, "primary")), 0);
}

#[test]
fn nested_fallback_remains_available_when_one_candidate_is_healthy() {
    let engine = engine(
        &["backup", "direct"],
        vec![json!({
            "tag": "backup", "type": "fallback", "outbounds": ["primary", "alternate"]
        })],
    );
    quarantine(&engine, "primary");
    assert!(matches!(
        resolve(&engine, "auto"),
        ResolvedOutbound::Fallback { .. }
    ));
}

#[test]
fn availability_checks_do_not_advance_nested_loadbalance_rotation() {
    let engine = engine(
        &["balanced", "direct"],
        vec![json!({
            "tag": "balanced", "type": "load_balance",
            "outbounds": ["primary", "alternate"], "strategy": "round_robin"
        })],
    );
    for expected in [0, 1, 0, 1] {
        let ResolvedOutbound::Fallback { candidates } = resolve(&engine, "auto") else {
            panic!("expected balanced candidates");
        };
        assert_eq!(
            single_index(ResolvedOutbound::Single(candidates[0].clone())),
            expected
        );
    }
}

#[test]
fn nested_urltest_can_use_its_healthy_member_without_leaving_the_outer_member() {
    let engine = engine(
        &["nested", "direct"],
        vec![json!({
            "tag": "nested", "type": "url_test", "outbounds": ["primary", "alternate"],
            "url": "http://probe.example/", "interval_seconds": 300
        })],
    );
    quarantine(&engine, "primary");
    assert_eq!(single_index(resolve(&engine, "auto")), 1);
}

#[test]
fn a_quarantined_relay_carrier_is_skipped_as_one_urltest_member() {
    let engine = engine(
        &["chain", "alternate"],
        vec![json!({
            "tag": "chain", "type": "relay", "proxies": ["primary", "alternate"]
        })],
    );
    assert!(matches!(
        resolve(&engine, "auto"),
        ResolvedOutbound::Relay { .. }
    ));
    quarantine(&engine, "primary");
    assert_eq!(single_index(resolve(&engine, "auto")), 1);
}
