use super::*;
use zero_api::{event_type, EventFilter, EventSource};
use zero_engine::{UrlTestMemberState, UrlTestSelectionReason};

#[test]
fn carrier_cooldown_immediately_changes_policy_state_and_emits_selection() {
    let engine = engine(&["primary", "alternate"], vec![]);
    let snapshot = engine.runtime_snapshot();
    let group = snapshot.plan().target_id("auto").unwrap();
    let primary = snapshot.plan().target_id("primary").unwrap();
    let alternate = snapshot.plan().target_id("alternate").unwrap();
    let measured = [(primary, 10), (alternate, 40)];
    let selection = snapshot
        .plan()
        .target(group)
        .unwrap()
        .as_urltest()
        .unwrap()
        .select(Some(primary), &measured);
    snapshot.update_urltest_state(
        group,
        primary,
        Some(10),
        measured
            .into_iter()
            .map(|(id, latency)| UrlTestMemberState {
                member_id: id,
                healthy: true,
                latency_ms: Some(latency),
                last_checked_unix_ms: Some(123),
                last_error: None,
                effective_chains: vec![vec![id]],
            })
            .collect(),
        selection,
    );
    let before = snapshot.urltest_state(group).unwrap();
    let events = engine
        .subscribe(EventFilter {
            event_types: vec![
                event_type::POLICY_SELECTED.to_owned(),
                event_type::POLICY_PROBE_COMPLETED.to_owned(),
            ],
            ..EventFilter::default()
        })
        .unwrap();

    quarantine(&engine, "primary");

    // No new flow or scheduled probe is needed to publish the change.
    let after = snapshot.urltest_state(group).unwrap();
    assert_eq!(after.selected, alternate);
    assert_eq!(after.latency_ms, Some(40));
    assert_eq!(after.last_checked_unix_ms, before.last_checked_unix_ms);
    assert_eq!(
        after.members, before.members,
        "preserve actual probe evidence"
    );
    assert_eq!(
        after.selection.unwrap().reason,
        UrlTestSelectionReason::CurrentUnhealthy
    );
    let event = events.try_recv().expect("immediate selection event");
    assert_eq!(event.event_type, event_type::POLICY_SELECTED);
    assert_eq!(event.payload["selected"], "alternate");
    assert_eq!(event.payload["previous"], "primary");
    assert!(
        events.try_recv().is_none(),
        "do not fabricate a probe completion"
    );
}

#[test]
fn isolated_failures_do_not_switch_urltest_or_mutate_probe_state() {
    let engine = engine(&["primary", "alternate"], vec![]);
    let before = engine.export_status().config.outbound_groups;
    engine.record_outbound_failure("primary");
    assert_eq!(engine.export_status().config.outbound_groups, before);
}

#[test]
fn carrier_failure_updates_all_urltest_groups_without_changing_pinned_selectors() {
    let engine = engine(
        &["primary", "alternate"],
        vec![
            json!({"tag":"other_auto", "type":"url_test", "outbounds":["primary","alternate"],
               "url":"http://probe.example/", "interval_seconds":300}),
            json!({"tag":"fixed", "type":"selector", "outbounds":["primary","alternate"]}),
        ],
    );
    quarantine(&engine, "primary");
    assert_eq!(single_index(resolve(&engine, "fixed")), 0);
    for group in engine.export_status().config.outbound_groups {
        assert_eq!(
            group.selected.as_deref(),
            Some(if group.tag == "fixed" {
                "primary"
            } else {
                "alternate"
            })
        );
    }
}

#[test]
fn late_probe_measurements_do_not_reselect_a_carrier_that_failed_after_its_probe() {
    let engine = engine(&["primary", "alternate"], vec![]);
    let snapshot = engine.runtime_snapshot();
    let group = snapshot.plan().target_id("auto").unwrap();
    quarantine(&engine, "primary");
    let events = engine
        .subscribe(EventFilter {
            event_types: vec![event_type::POLICY_SELECTED.to_owned()],
            ..EventFilter::default()
        })
        .unwrap();
    let members = ["primary", "alternate"]
        .into_iter()
        .enumerate()
        .map(|(index, tag)| UrlTestMemberState {
            member_id: snapshot.plan().target_id(tag).unwrap(),
            healthy: true,
            latency_ms: Some(10 + 20 * index as u64),
            last_checked_unix_ms: Some(123),
            last_error: None,
            effective_chains: vec![],
        })
        .collect();

    let state = engine
        .apply_urltest_probe_result(&snapshot, group, members)
        .unwrap();

    assert_eq!(
        state.selected,
        snapshot.plan().target_id("alternate").unwrap()
    );
    assert!(
        state.members.iter().all(|member| member.healthy),
        "keep actual measurements"
    );
    assert!(
        events.try_recv().is_none(),
        "unchanged selection produces no switch event"
    );
}

#[test]
fn retired_generation_results_cannot_change_current_health_or_selection() {
    let engine = engine(&["primary", "alternate"], vec![]);
    let old = engine.runtime_snapshot();
    engine
        .reload_runtime_config(old.config().as_ref().clone())
        .unwrap();
    let before = engine.export_status().config.outbound_groups;
    for _ in 0..5 {
        engine.record_outbound_failure_in_snapshot(&old, "primary");
    }
    engine
        .check_outbound_health("primary")
        .expect("ignore retired failures");
    assert_eq!(engine.export_status().config.outbound_groups, before);
    quarantine(&engine, "primary");
    engine.record_outbound_success_in_snapshot(&old, "primary");
    engine
        .check_outbound_health("primary")
        .expect_err("ignore retired success");
    assert!(engine
        .apply_urltest_probe_result(&old, old.plan().target_id("auto").unwrap(), vec![])
        .is_none());
}
