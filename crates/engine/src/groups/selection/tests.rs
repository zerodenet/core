use super::*;
use crate::{Engine, ResolvedLeafOutbound, ResolvedOutbound};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Wake;

fn make_engine() -> Engine {
    Engine::new(zero_config::RuntimeConfig::parse(&serde_json::json!({
        "outbounds":[{"tag":"a","protocol":{"type":"direct"}},{"tag":"b","protocol":{"type":"direct"}}],
        "outbound_groups":[
            {"tag":"choose","type":"selector","outbounds":["a","b"],"selected":"a"},
            {"tag":"auto","type":"url_test","outbounds":["a","b"]},
            {"tag":"balance","type":"load_balance","outbounds":["a","b"],"strategy":"round_robin"}
        ], "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap()).unwrap()
}
#[derive(Default)]
struct Wakes(AtomicUsize);
impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}
fn poll(change: &mut SelectionChange, wakes: &Arc<Wakes>) -> Poll<GroupSelectionChanged> {
    let waker = Waker::from(wakes.clone());
    Pin::new(change).poll(&mut Context::from_waker(&waker))
}

#[test]
fn actual_selector_changes_notify_without_aba_or_same_value_invalidations() {
    let engine = make_engine();
    let snapshot = engine.runtime_snapshot();
    let binding = snapshot
        .group_selection_binding(snapshot.plan().target_id("choose").unwrap())
        .unwrap();
    assert_eq!(
        snapshot.plan().target(binding.selected()).unwrap().tag(),
        "a"
    );
    let wakes = Arc::new(Wakes::default());
    let mut changed = binding.changed();
    assert!(poll(&mut changed, &wakes).is_pending());
    engine.set_selector_target("choose", "a").unwrap();
    assert_eq!(wakes.0.load(Ordering::Acquire), 0);
    binding.check_current().unwrap();
    engine.set_selector_target("choose", "b").unwrap();
    assert_eq!(wakes.0.load(Ordering::Acquire), 1);
    engine.set_selector_target("choose", "a").unwrap();
    assert_eq!(binding.check_current(), Err(GroupSelectionChanged::Changed));
    assert_eq!(
        poll(&mut changed, &wakes),
        Poll::Ready(GroupSelectionChanged::Changed)
    );
    let fresh = snapshot
        .group_selection_binding(snapshot.plan().target_id("choose").unwrap())
        .unwrap();
    assert_eq!(fresh.selected(), binding.selected());
    assert!(!fresh.same_capture(&binding));
    fresh.check_current().unwrap();
}

#[test]
fn change_before_first_poll_is_not_lost_and_dropped_waiter_has_no_strong_registration() {
    let engine = make_engine();
    let snapshot = engine.runtime_snapshot();
    let binding = snapshot
        .group_selection_binding(snapshot.plan().target_id("choose").unwrap())
        .unwrap();
    let wakes = Arc::new(Wakes::default());
    let mut dropped = binding.changed();
    assert!(poll(&mut dropped, &wakes).is_pending());
    drop(dropped);
    let mut not_polled = binding.changed();
    engine.set_selector_target("choose", "b").unwrap();
    assert_eq!(wakes.0.load(Ordering::Acquire), 0);
    assert_eq!(
        poll(&mut not_polled, &wakes),
        Poll::Ready(GroupSelectionChanged::Changed)
    );
}

#[test]
fn urltest_latency_only_updates_preserve_binding_but_actual_selected_updates_invalidate() {
    let engine = make_engine();
    let snapshot = engine.runtime_snapshot();
    let group_id = snapshot.plan().target_id("auto").unwrap();
    let group = snapshot
        .plan()
        .target(group_id)
        .unwrap()
        .as_urltest()
        .unwrap();
    let binding = snapshot.group_selection_binding(group_id).unwrap();
    let selected = binding.selected();
    let other = *group
        .members()
        .iter()
        .find(|member| **member != selected)
        .unwrap();
    let same = group.select(Some(selected), &[(selected, 10), (other, 100)]);
    snapshot.update_urltest_state(group_id, same.selected, Some(10), vec![], same);
    binding.check_current().unwrap();
    let expected = snapshot.urltest_state(group_id).unwrap();
    let switch = group.select(Some(selected), &[(selected, 100), (other, 10)]);
    assert!(snapshot
        .outbound_group_state
        .switch_urltest(group_id, &expected, switch));
    assert_eq!(binding.check_current(), Err(GroupSelectionChanged::Changed));
    let fresh = snapshot.group_selection_binding(group_id).unwrap();
    let stale_update = group.select(Some(other), &[(selected, 1), (other, 200)]);
    assert!(!snapshot
        .outbound_group_state
        .switch_urltest(group_id, &expected, stale_update));
    fresh.check_current().unwrap();
    let changed = group.select(Some(other), &[(selected, 1), (other, 200)]);
    snapshot.update_urltest_state(group_id, changed.selected, Some(1), vec![], changed);
    assert_eq!(fresh.check_current(), Err(GroupSelectionChanged::Changed));
}

#[test]
fn captures_have_actual_state_identity_and_checks_do_not_advance_round_robin() {
    let engine = make_engine();
    let other = make_engine();
    let snapshot = engine.runtime_snapshot();
    let other = other.runtime_snapshot();
    let selector = snapshot.plan().target_id("choose").unwrap();
    let binding = snapshot.group_selection_binding(selector).unwrap();
    assert!(!binding.same_capture(
        &other
            .group_selection_binding(other.plan().target_id("choose").unwrap())
            .unwrap()
    ));
    let lb = snapshot.plan().target_id("balance").unwrap();
    let first = engine
        .resolve_target_id_in_snapshot(&snapshot, lb)
        .unwrap()
        .0;
    for _ in 0..4 {
        assert!(snapshot.group_selection_binding(lb).is_none());
        binding.check_current().unwrap();
    }
    let second = engine
        .resolve_target_id_in_snapshot(&snapshot, lb)
        .unwrap()
        .0;
    let first_tag = |resolved: ResolvedOutbound<'_>| match resolved {
        ResolvedOutbound::Fallback { candidates } => match candidates.into_iter().next().unwrap() {
            ResolvedLeafOutbound::Direct { tag, .. } => tag.unwrap().to_owned(),
            _ => panic!("direct fixture"),
        },
        _ => panic!("loadbalance is an ordered fallback"),
    };
    assert_eq!(first_tag(first), "a");
    assert_eq!(first_tag(second), "b");
}

#[test]
fn checked_revision_exhaustion_never_wraps_or_revives_a_capture() {
    let engine = make_engine();
    let snapshot = engine.runtime_snapshot();
    let binding = snapshot
        .group_selection_binding(snapshot.plan().target_id("choose").unwrap())
        .unwrap();
    // Explicit fault injection at the actual signal's finite revision bound;
    // no network or resource generation is fabricated by this test.
    binding.signal.0.lock().unwrap().revision = u64::MAX;
    engine.set_selector_target("choose", "b").unwrap();
    assert_eq!(
        binding.check_current(),
        Err(GroupSelectionChanged::RevisionExhausted)
    );
    let fresh = snapshot
        .group_selection_binding(snapshot.plan().target_id("choose").unwrap())
        .unwrap();
    assert_eq!(
        fresh.check_current(),
        Err(GroupSelectionChanged::RevisionExhausted)
    );
}
