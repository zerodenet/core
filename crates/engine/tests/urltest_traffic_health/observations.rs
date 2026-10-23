use super::*;
use std::sync::Arc;
use zero_api::{event_type, EventFilter, EventSource};

#[test]
fn bound_carrier_failures_immediately_switch_policy_and_success_restores_health() {
    let engine = engine(&["primary", "alternate"], vec![]);
    let snapshot = engine.runtime_snapshot();
    let observation = engine.bind_outbound_health(&snapshot, "primary");
    let events = engine
        .subscribe(EventFilter {
            event_types: vec![event_type::POLICY_SELECTED.to_owned()],
            ..Default::default()
        })
        .unwrap();
    for _ in 0..5 {
        observation.record_failure();
    }
    engine.check_outbound_health("primary").unwrap_err();
    let group = snapshot.plan().target_id("auto").unwrap();
    assert_eq!(
        snapshot.urltest_selected_target(group),
        snapshot.plan().target_id("alternate")
    );
    assert!(events.try_recv().is_some());
    assert!(events.try_recv().is_none());
    observation.record_success();
    engine.check_outbound_health("primary").unwrap();
    assert_eq!(
        snapshot.urltest_selected_target(group),
        snapshot.plan().target_id("alternate")
    );
}

#[test]
fn bound_observer_does_not_retain_retired_config_or_change_current_health() {
    let engine = engine(&["primary", "alternate"], vec![]);
    let old = engine.runtime_snapshot();
    let config = Arc::downgrade(old.config());
    let observation = engine.bind_outbound_health(&old, "primary");
    engine
        .reload_runtime_config(old.config().as_ref().clone())
        .unwrap();
    drop(old);
    assert!(
        config.upgrade().is_none(),
        "observer must release historical config"
    );
    let before = engine.export_status().config.outbound_groups;
    for _ in 0..5 {
        observation.record_failure();
    }
    engine.check_outbound_health("primary").unwrap();
    assert_eq!(engine.export_status().config.outbound_groups, before);
    quarantine(&engine, "primary");
    observation.record_success();
    engine.check_outbound_health("primary").unwrap_err();
}

#[test]
fn retained_health_observer_does_not_keep_a_stopped_engine_alive() {
    let engine = engine(&["primary", "alternate"], vec![]);
    let snapshot = engine.runtime_snapshot();
    let config = Arc::downgrade(snapshot.config());
    let observation = engine.bind_outbound_health(&snapshot, "primary");
    drop(snapshot);
    drop(engine);
    assert!(config.upgrade().is_none());
    observation.record_failure();
    observation.record_success();
}
