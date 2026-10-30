use super::*;
use crate::{Engine, RouteDecision, RouteMode};
use serde_json::{json, Value};
use zero_config::RuntimeConfig;
use zero_core::Address;

fn config(final_action: Value) -> RuntimeConfig {
    RuntimeConfig::parse(
        &json!({
            "inbounds": [{"tag":"special",
                "listen":{"address":"127.0.0.1","port":18080},
                "protocol":{"type":"socks5"}}],
            "outbounds": [
                {"tag":"a","protocol":{"type":"direct"}},
                {"tag":"b","protocol":{"type":"direct"}}
            ],
            "route": {
                "bypass": [{"type":"ip","values":["192.168.0.0/16"]}],
                "rules": [
                    {"condition":{"type":"inbound","values":["special"]},
                     "action":{"type":"reject"},"mode":"packet"},
                    {"condition":{"type":"ip","values":["10.0.0.0/8","2001:db8::/32"]},
                     "action":{"type":"route","outbound":"a"},"mode":"translate"}
                ],
                "final": final_action,
                "final_mode":"flow"
            }
        })
        .to_string(),
    )
    .unwrap()
}

fn evaluate(
    engine: &Engine,
    snapshot: &Arc<EngineRuntimeSnapshot>,
    cache: &mut IpRouteCache,
    address: &str,
    inbound: Option<&str>,
) -> RouteEvaluation {
    engine.evaluate_ip_route_cached_in_snapshot(snapshot, address.parse().unwrap(), inbound, cache)
}

#[test]
fn repeated_ip_reuses_evaluation_and_preserves_full_trace() {
    let engine = Engine::new(config(json!({"type":"direct"}))).unwrap();
    let snapshot = engine.runtime_snapshot();
    let mut cache = IpRouteCache::default();
    for inbound in [None, Some("ordinary"), Some("special")] {
        for ip in ["10.1.2.3", "2001:db8::1", "192.168.1.1", "203.0.113.9"] {
            let address: IpAddr = ip.parse().unwrap();
            let destination = match address {
                IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
                IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
            };
            let expected =
                engine.evaluate_route_in_snapshot(&snapshot, &destination, None, inbound, &[]);
            let before = cache.evaluations;
            for _ in 0..4 {
                assert_eq!(
                    evaluate(&engine, &snapshot, &mut cache, ip, inbound),
                    expected
                );
            }
            assert_eq!(cache.evaluations, before + 1);
        }
    }
    assert_eq!(cache.hits, 36);
}

#[test]
fn different_addresses_and_inbound_contexts_do_not_share_decisions() {
    let engine = Engine::new(config(json!({"type":"direct"}))).unwrap();
    let snapshot = engine.runtime_snapshot();
    let mut cache = IpRouteCache::default();
    let first = evaluate(&engine, &snapshot, &mut cache, "10.1.2.3", None);
    assert_eq!(first.trace.decision, RouteDecision::Route("a".into()));
    assert_eq!(first.trace.route_mode, RouteMode::Translate);
    assert_eq!(
        evaluate(&engine, &snapshot, &mut cache, "203.0.113.9", None)
            .trace
            .decision,
        RouteDecision::Direct
    );
    let other = evaluate(&engine, &snapshot, &mut cache, "10.1.2.3", Some("special"));
    assert_eq!(other.trace.decision, RouteDecision::Reject);
    assert_eq!(other.trace.route_mode, RouteMode::Packet);
    assert_eq!(
        evaluate(&engine, &snapshot, &mut cache, "10.1.2.3", None),
        first
    );
    assert_eq!(cache.evaluations, 4);
}

#[test]
fn same_revision_staging_and_rollback_use_the_actual_snapshot_identity() {
    let engine = Engine::new(config(json!({"type":"direct"}))).unwrap();
    let original = engine.runtime_snapshot();
    let mut cache = IpRouteCache::default();
    let first = evaluate(&engine, &original, &mut cache, "203.0.113.9", None);
    engine
        .stage_runtime_config(config(json!({"type":"reject"})))
        .unwrap();
    let staged = engine.runtime_snapshot();
    assert_eq!(original.config_revision(), staged.config_revision());
    assert_eq!(
        evaluate(&engine, &staged, &mut cache, "203.0.113.9", None)
            .trace
            .decision,
        RouteDecision::Reject
    );
    engine
        .restore_staged_snapshot(original.clone(), false)
        .unwrap();
    let restored = engine.runtime_snapshot();
    assert!(Arc::ptr_eq(&original, &restored));
    assert_eq!(
        evaluate(&engine, &restored, &mut cache, "203.0.113.9", None),
        first
    );
    assert_eq!(cache.evaluations, 3);
}

#[test]
fn endpoint_override_replaces_cache_epoch_even_when_config_and_revision_are_shared() {
    let config = RuntimeConfig::parse(
        &json!({
            "endpoints":[{"tag":"endpoint-target","protocol":{
                "type":"wireguard","private_key":"01".repeat(32),
                "addresses":["10.0.0.1/32"],
                "peers":[{"public_key":"02".repeat(32),
                    "endpoint":"127.0.0.1:51821","allowed_ips":["10.0.0.0/24"]}]
            }}],
            "route":{"rules":[],"final":{"type":"route","outbound":"endpoint-target"}}
        })
        .to_string(),
    )
    .unwrap();
    let engine = Engine::new(config).unwrap();
    let original = engine.runtime_snapshot();
    let mut cache = IpRouteCache::default();
    let first = evaluate(&engine, &original, &mut cache, "203.0.113.9", None);
    assert!(engine
        .resolve_route_decision_in_snapshot(&original, first.trace.decision.clone())
        .is_ok());
    let disabled = engine
        .prepare_endpoint_change(
            &original,
            "endpoint:endpoint-target",
            crate::EndpointChange::Enabled(false),
            false,
            None,
        )
        .unwrap();
    assert!(Arc::ptr_eq(original.config(), disabled.config()));
    assert_eq!(original.config_revision(), disabled.config_revision());
    let after = evaluate(&engine, &disabled, &mut cache, "203.0.113.9", None);
    assert_eq!(after, first);
    assert_eq!(cache.evaluations, 2);
    assert!(engine
        .resolve_route_decision_in_snapshot(&disabled, after.trace.decision)
        .is_err());
}

#[test]
fn mode_changes_on_a_retained_snapshot_include_the_global_target() {
    let engine = Engine::new(config(json!({"type":"reject"}))).unwrap();
    let retained = engine.runtime_snapshot();
    let mut cache = IpRouteCache::default();
    for (mode, expected) in [
        (ModeConfig::Rule, RouteDecision::Reject),
        (ModeConfig::Direct, RouteDecision::Direct),
        (
            ModeConfig::Global {
                outbound: "a".into(),
            },
            RouteDecision::Route("a".into()),
        ),
        (
            ModeConfig::Global {
                outbound: "b".into(),
            },
            RouteDecision::Route("b".into()),
        ),
        (
            ModeConfig::Global {
                outbound: "a".into(),
            },
            RouteDecision::Route("a".into()),
        ),
    ] {
        engine.set_mode(mode);
        for _ in 0..2 {
            assert_eq!(
                evaluate(&engine, &retained, &mut cache, "203.0.113.9", None)
                    .trace
                    .decision,
                expected
            );
        }
    }
    assert_eq!(cache.evaluations, 5);
    assert_eq!(cache.hits, 5);
}

#[test]
fn fifo_eviction_bounds_unique_destinations_without_scanning_on_hits() {
    let engine = Engine::new(config(json!({"type":"direct"}))).unwrap();
    let snapshot = engine.runtime_snapshot();
    let mut cache = IpRouteCache::default();
    for index in 0..=MAX_CACHED_IP_ROUTES {
        let address = IpAddr::V4(std::net::Ipv4Addr::from(0xcb00_0000 + index as u32));
        engine.evaluate_ip_route_cached_in_snapshot(&snapshot, address, None, &mut cache);
        assert!(cache.entries.len() <= MAX_CACHED_IP_ROUTES);
        assert_eq!(cache.order.len(), cache.entries.len());
    }
    assert!(!cache.entries.contains_key(&"203.0.0.0".parse().unwrap()));
    assert!(cache.entries.contains_key(&"203.0.0.1".parse().unwrap()));
    let before = cache.evaluations;
    evaluate(&engine, &snapshot, &mut cache, "203.0.0.1", None);
    assert_eq!(cache.evaluations, before);
    evaluate(&engine, &snapshot, &mut cache, "203.0.0.0", None);
    assert_eq!(cache.evaluations, before + 1);
    assert_eq!(cache.entries.len(), MAX_CACHED_IP_ROUTES);
    assert_eq!(cache.order.len(), MAX_CACHED_IP_ROUTES);
}

#[test]
fn retaining_the_cache_does_not_retain_snapshot_or_configuration_contents() {
    let engine = Engine::new(config(json!({"type":"direct"}))).unwrap();
    let snapshot = engine.runtime_snapshot();
    let weak_snapshot = Arc::downgrade(&snapshot);
    let weak_config = Arc::downgrade(snapshot.config());
    let mut cache = IpRouteCache::default();
    evaluate(&engine, &snapshot, &mut cache, "10.1.2.3", None);
    drop(snapshot);
    drop(engine);
    assert!(weak_snapshot.upgrade().is_none());
    assert!(weak_config.upgrade().is_none());
    assert_eq!(cache.entries.len(), 1);
}
