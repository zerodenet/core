use zero_config::RuntimeConfig;
use zero_engine::{Engine, EnginePlan, TargetId, UrlTestSelectionReason};

#[test]
fn tolerance_keeps_or_switches_at_the_strict_boundary() {
    let (plan, group_id, node_a, node_b) = plan_with_tolerance(Some(20));
    let urltest = plan.target(group_id).unwrap().as_urltest().unwrap();

    let within = urltest.select(Some(node_a), &[(node_a, 82), (node_b, 75)]);
    assert_eq!(within.selected, node_a);
    assert!(!within.switched);
    assert_eq!(within.reason, UrlTestSelectionReason::WithinTolerance);

    let boundary = urltest.select(Some(node_a), &[(node_a, 95), (node_b, 75)]);
    assert_eq!(boundary.selected, node_a);
    assert_eq!(boundary.reason, UrlTestSelectionReason::WithinTolerance);

    let beyond = urltest.select(Some(node_a), &[(node_a, 96), (node_b, 75)]);
    assert_eq!(beyond.selected, node_b);
    assert!(beyond.switched);
    assert_eq!(beyond.reason, UrlTestSelectionReason::BetterBeyondTolerance);
}

#[test]
fn failure_recovery_ties_and_all_failed_are_deterministic() {
    let (plan, group_id, node_a, node_b) = plan_with_tolerance(Some(20));
    let urltest = plan.target(group_id).unwrap().as_urltest().unwrap();

    let failed_current = urltest.select(Some(node_a), &[(node_b, 120)]);
    assert_eq!(failed_current.selected, node_b);
    assert_eq!(
        failed_current.reason,
        UrlTestSelectionReason::CurrentUnhealthy
    );

    let recovered = urltest.select(Some(node_b), &[(node_a, 75), (node_b, 120)]);
    assert_eq!(recovered.selected, node_a);
    assert_eq!(
        recovered.reason,
        UrlTestSelectionReason::BetterBeyondTolerance
    );

    let tie = urltest.select(Some(node_b), &[(node_a, 75), (node_b, 75)]);
    assert_eq!(tie.selected, node_b);
    assert_eq!(tie.best, Some(node_a));
    assert_eq!(tie.reason, UrlTestSelectionReason::WithinTolerance);

    let all_failed = urltest.select(Some(node_b), &[]);
    assert_eq!(all_failed.selected, node_b);
    assert_eq!(all_failed.reason, UrlTestSelectionReason::NoHealthyMember);

    let initial = urltest.select(None, &[(node_a, 80), (node_b, 70)]);
    assert_eq!(initial.selected, node_b);
    assert_eq!(initial.reason, UrlTestSelectionReason::Initial);
}

#[test]
fn omitted_tolerance_preserves_strict_best_and_reload_updates_the_plan() {
    let config = config_with_tolerance(None);
    let engine = Engine::new(config).expect("build engine");
    let old = engine.runtime_snapshot();
    let group_id = old.plan().target_id("auto").unwrap();
    let node_a = old.plan().target_id("node-a").unwrap();
    let node_b = old.plan().target_id("node-b").unwrap();
    let old_urltest = old.plan().target(group_id).unwrap().as_urltest().unwrap();
    assert_eq!(old_urltest.tolerance_ms(), 0);
    assert_eq!(
        old_urltest
            .select(Some(node_a), &[(node_a, 82), (node_b, 81)])
            .selected,
        node_b
    );

    engine
        .reload_runtime_config(config_with_tolerance(Some(25)))
        .expect("reload tolerance");
    let new = engine.runtime_snapshot();
    let new_group = new.plan().target_id("auto").unwrap();
    let new_urltest = new.plan().target(new_group).unwrap().as_urltest().unwrap();
    assert_eq!(new.config_revision(), 2);
    assert_eq!(new_urltest.tolerance_ms(), 25);
}

fn plan_with_tolerance(tolerance_ms: Option<u64>) -> (EnginePlan, TargetId, TargetId, TargetId) {
    let config = config_with_tolerance(tolerance_ms);
    let plan = EnginePlan::build(&config).expect("build plan");
    let group_id = plan.target_id("auto").unwrap();
    let node_a = plan.target_id("node-a").unwrap();
    let node_b = plan.target_id("node-b").unwrap();
    (plan, group_id, node_a, node_b)
}

fn config_with_tolerance(tolerance_ms: Option<u64>) -> RuntimeConfig {
    let tolerance = tolerance_ms
        .map(|value| format!(", \"tolerance_ms\": {value}"))
        .unwrap_or_default();
    RuntimeConfig::parse(&format!(
        r#"{{
            "outbounds": [
                {{ "tag": "node-a", "protocol": {{ "type": "direct" }} }},
                {{ "tag": "node-b", "protocol": {{ "type": "direct" }} }}
            ],
            "outbound_groups": [{{
                "tag": "auto",
                "type": "url_test",
                "outbounds": ["node-a", "node-b"],
                "url": "http://example.com/",
                "interval_seconds": 60
                {tolerance}
            }}],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#
    ))
    .expect("parse config")
}
