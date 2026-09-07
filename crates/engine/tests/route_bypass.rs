use serde_json::{json, Value};
use zero_config::{ModeConfig, RuntimeConfig};
use zero_core::Address;
use zero_engine::{Engine, RouteDecision};

fn config(bypass: Value) -> RuntimeConfig {
    RuntimeConfig::parse(
        &json!({
            "outbounds":[{"tag":"proxy","protocol":{"type":"direct"}}],
            "route":{"bypass":bypass,"rules":[],"final":{"type":"reject"}}
        })
        .to_string(),
    )
    .unwrap()
}

#[test]
fn bypass_is_direct_in_rule_and_global_modes_and_survives_mode_changes() {
    let engine = Engine::new(config(json!([
        {"type":"ip","values":["192.168.0.0/16"]},
        {"type":"domain_regex","values":["^.*\\.internal\\.example$"]}
    ])))
    .unwrap();
    for mode in [
        ModeConfig::Rule,
        ModeConfig::Global {
            outbound: "proxy".into(),
        },
        ModeConfig::Direct,
    ] {
        engine.set_mode(mode);
        for address in [
            Address::Ipv4([192, 168, 0, 1]),
            Address::Domain("api.internal.example".into()),
        ] {
            assert_eq!(
                engine
                    .route_trace_with_inbound_and_resolved_ips(&address, None, None, &[])
                    .decision,
                RouteDecision::Direct
            );
        }
    }
    engine.set_mode(ModeConfig::Global {
        outbound: "proxy".into(),
    });
    assert_eq!(
        engine
            .route_trace_with_inbound_and_resolved_ips(
                &Address::Domain("public.example".into()),
                None,
                None,
                &[]
            )
            .decision,
        RouteDecision::Route("proxy".into())
    );
}

#[test]
fn bypass_uses_resolved_addresses_in_global_mode_without_changing_unmatched_traffic() {
    let engine = Engine::new(config(json!([{"type":"ip","values":["192.168.0.0/16"]}]))).unwrap();
    engine.set_mode(ModeConfig::Global {
        outbound: "proxy".into(),
    });
    assert!(engine.route_requires_resolved_ip());
    let address = Address::Domain("router.example".into());
    let ips = ["192.168.0.1".parse().unwrap()];
    assert_eq!(
        engine
            .route_trace_with_inbound_and_resolved_ips(&address, None, None, &ips)
            .decision,
        RouteDecision::Direct
    );
}

#[test]
fn reloading_bypass_does_not_change_old_snapshot_and_rejects_invalid_conditions() {
    let engine = Engine::new(config(json!([{"type":"ip","values":["192.168.0.0/16"]}]))).unwrap();
    let old = engine.runtime_snapshot();
    engine.reload_runtime_config(config(json!([]))).unwrap();
    let address = Address::Ipv4([192, 168, 0, 1]);
    assert_eq!(
        engine
            .route_trace_in_snapshot_with_inbound_and_resolved_ips(&old, &address, None, None, &[])
            .decision,
        RouteDecision::Direct
    );
    assert_eq!(
        engine
            .route_trace_with_inbound_and_resolved_ips(&address, None, None, &[])
            .decision,
        RouteDecision::Reject
    );
    assert!(RuntimeConfig::parse(&json!({"route":{"bypass":[{"type":"domain_regex","values":["["]}],"final":{"type":"direct"}}}).to_string()).is_err());
}

#[test]
fn domain_bypass_is_final_even_when_both_rule_sets_contain_ip_conditions() {
    let mut config = config(json!([
        {"type":"domain", "values":["router.example"]},
        {"type":"sni", "values":["router.example"]},
        {"type":"ip", "values":["192.168.0.0/16"]}
    ]));
    config.route.rules = serde_json::from_value(json!([{
        "condition":{"type":"ip","values":["0.0.0.0/0"]},
        "action":{"type":"reject"}
    }]))
    .unwrap();
    let engine = Engine::new(config).unwrap();
    for mode in [
        ModeConfig::Rule,
        ModeConfig::Global {
            outbound: "proxy".into(),
        },
        ModeConfig::Direct,
    ] {
        engine.set_mode(mode);
        let snapshot = engine.runtime_snapshot();
        for (address, sni) in [
            (Address::Domain("router.example".into()), None),
            (
                Address::Domain("alias.example".into()),
                Some("router.example"),
            ),
        ] {
            let result = engine.evaluate_route_in_snapshot(&snapshot, &address, sni, None, &[]);
            assert_eq!(result.trace.decision, RouteDecision::Direct);
            assert!(!result.needs_resolution);
        }
    }
}

#[test]
fn ordinary_domain_match_still_requires_ip_bypass_resolution() {
    let mut config = config(json!([{"type":"ip","values":["192.168.0.0/16"]}]));
    config.route.rules = serde_json::from_value(json!([{
        "condition":{"type":"domain","values":["router.example"]},
        "action":{"type":"reject"}
    }]))
    .unwrap();
    let engine = Engine::new(config).unwrap();
    for mode in [
        ModeConfig::Rule,
        ModeConfig::Global {
            outbound: "proxy".into(),
        },
    ] {
        engine.set_mode(mode);
        let snapshot = engine.runtime_snapshot();
        let address = Address::Domain("router.example".into());
        let unresolved = engine.evaluate_route_in_snapshot(&snapshot, &address, None, None, &[]);
        assert!(unresolved.needs_resolution);
        let resolved = engine.evaluate_route_in_snapshot(
            &snapshot,
            &address,
            None,
            None,
            &["192.168.0.1".parse().unwrap()],
        );
        assert_eq!(resolved.trace.decision, RouteDecision::Direct);
        assert!(!resolved.needs_resolution);
    }
}

#[test]
fn a_direct_fallback_is_not_confused_with_a_final_bypass() {
    let config = RuntimeConfig::parse(&json!({"route":{
        "rules":[{"condition":{"type":"ip","values":["192.168.0.0/16"]},"action":{"type":"reject"}}],
        "final":{"type":"direct"}
    }}).to_string()).unwrap();
    let engine = Engine::new(config).unwrap();
    let snapshot = engine.runtime_snapshot();
    let address = Address::Domain("router.example".into());
    let result = engine.evaluate_route_in_snapshot(&snapshot, &address, None, None, &[]);
    assert_eq!(result.trace.decision, RouteDecision::Direct);
    assert!(result.needs_resolution);
    let result = engine.evaluate_route_in_snapshot(
        &snapshot,
        &address,
        None,
        None,
        &["192.168.0.1".parse().unwrap()],
    );
    assert_eq!(result.trace.decision, RouteDecision::Reject);
    assert!(!result.needs_resolution);
}
