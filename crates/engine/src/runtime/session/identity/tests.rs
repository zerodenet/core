use super::*;
use zero_api::{event_type, EventFilter};
use zero_core::{Network, ProtocolType};
mod policy;

fn new_engine() -> Engine {
    Engine::new(
        zero_config::RuntimeConfig::parse(r#"{"inbounds":[],"route":{"final":{"type":"direct"}}}"#)
            .unwrap(),
    )
    .unwrap()
}
fn session(id: SessionId) -> Session {
    Session::new(
        id.0,
        Address::Domain("identity.test".into()),
        443,
        Network::Tcp,
        ProtocolType::new("test-auth"),
    )
}
fn events(engine: &Engine, kind: &str) -> Vec<zero_api::RawApiEvent> {
    engine.events_snapshot(&EventFilter {
        event_types: vec![kind.to_owned()],
        ..EventFilter::default()
    })
}
fn no_business(engine: &Engine) {
    assert!(engine.active_sessions().is_empty());
    assert!(engine.completed_sessions().is_empty());
    assert!(events(engine, event_type::FLOW_STARTED).is_empty());
    assert!(events(engine, event_type::FLOW_COMPLETED).is_empty());
}
fn refused(result: Result<(), EngineError>, expected: &str) {
    assert!(
        matches!(&result, Err(EngineError::AdmissionDenied { reason }) if reason.contains(expected)),
        "{result:?}"
    );
}

#[test]
fn minted_identity_is_preserved_through_real_registry_events_and_single_settlement() {
    let engine = new_engine();
    let identity = engine.mint_session_identity().unwrap();
    no_business(&engine);
    let mut business = session(identity.id());
    let mut auth = SessionAuth::new("test-auth");
    auth.up_bps = Some(123);
    auth.down_bps = Some(456);
    business.apply_auth(auth.clone());
    engine
        .clone()
        .prepare_session_with_identity(&identity, &mut business, "in")
        .unwrap();
    assert_eq!(business.id, identity.id().0);
    let active = engine.active_sessions();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].id, identity.id().0);
    assert_eq!(active[0].auth, Some(auth.clone()));
    let started = events(&engine, event_type::FLOW_STARTED);
    assert_eq!(started.len(), 1);
    assert_eq!(started[0].payload["flow_id"], identity.id().0.to_string());
    assert_eq!(started[0].payload["auth"]["scheme"], "test-auth");
    assert!(started[0].payload["auth"]["principal_key"].is_null());
    let mut handle = engine.track_session(identity.id().0);
    let completed = handle.finish(SessionOutcome::DirectRelayed).unwrap();
    assert_eq!(completed.id, identity.id().0);
    assert_eq!(completed.auth, Some(auth));
    assert!(handle.finish(SessionOutcome::Failed).is_none());
    assert!(engine.active_sessions().is_empty());
    assert_eq!(engine.completed_sessions().len(), 1);
    let completed = events(&engine, event_type::FLOW_COMPLETED);
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].payload["flow_id"], identity.id().0.to_string());
    assert_eq!(completed[0].payload["auth"]["scheme"], "test-auth");
}

#[test]
fn a_ticket_cannot_reuse_an_active_or_settled_identity() {
    let engine = new_engine();
    let identity = engine.mint_session_identity().unwrap();
    let mut business = session(identity.id());
    engine
        .prepare_session_with_identity(&identity, &mut business, "in")
        .unwrap();
    refused(
        engine.prepare_session_with_identity(&identity, &mut business, "other"),
        "already used",
    );
    assert_eq!(business.inbound_tag.as_deref(), Some("in"));
    assert_eq!(engine.active_sessions().len(), 1);
    engine
        .track_session(business.id)
        .finish(SessionOutcome::DirectRelayed)
        .unwrap();
    refused(
        engine.prepare_session_with_identity(&identity, &mut business, "other"),
        "already used",
    );
    assert!(engine.active_sessions().is_empty());
    assert_eq!(engine.completed_sessions().len(), 1);
    assert_eq!(events(&engine, event_type::FLOW_STARTED).len(), 1);
}

#[test]
fn foreign_and_mismatched_ids_have_no_effect_and_do_not_consume_the_real_ticket() {
    let first = new_engine();
    let other = new_engine();
    let identity = first.mint_session_identity().unwrap();
    let foreign = other.mint_session_identity().unwrap();
    assert_eq!(
        identity.id(),
        foreign.id(),
        "numeric equality is no issuer proof"
    );
    let mut business = session(identity.id());
    refused(
        other.prepare_session_with_identity(&identity, &mut business, "foreign"),
        "different Engine",
    );
    assert!(business.inbound_tag.is_none());
    no_business(&other);
    let mut mismatch = session(SessionId(identity.id().0 + 99));
    let before = mismatch.clone();
    refused(
        first.prepare_session_with_identity(&identity, &mut mismatch, "mismatch"),
        "does not match",
    );
    assert_eq!(mismatch, before);
    no_business(&first);
    first
        .prepare_session_with_identity(&identity, &mut business, "real")
        .unwrap();
    first
        .track_session(business.id)
        .finish(SessionOutcome::DirectRelayed)
        .unwrap();
}

#[test]
fn cancelling_an_unadmitted_owner_drops_only_the_ticket_without_registering_history() {
    let engine = new_engine();
    let before = Arc::strong_count(&engine.next_session_id);
    let weak_before = Arc::weak_count(&engine.next_session_id);
    let identity = engine.mint_session_identity().unwrap();
    assert_eq!(
        Arc::strong_count(&engine.next_session_id),
        before,
        "ticket is no Engine owner"
    );
    let cancelled_id = identity.id();
    let same_engine = &engine;
    let mut waiting = Box::pin(async move {
        std::future::pending::<()>().await;
        let mut business = session(identity.id());
        same_engine.prepare_session_with_identity(&identity, &mut business, "cancelled")
    });
    use std::{
        future::Future,
        task::{Context, Waker},
    };
    assert!(waiting
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    no_business(&engine);
    drop(waiting); // Cancel an actual pending owner future, dropping its ticket.
    assert_eq!(
        Arc::weak_count(&engine.next_session_id),
        weak_before,
        "no allocator-held pending/used-ticket history remains"
    );
    no_business(&engine);
    let next = engine.mint_session_identity().unwrap();
    assert_ne!(next.id(), cancelled_id, "abandoned ids are not recycled");
    let issuer = next.issuer.clone();
    drop(engine);
    assert!(
        issuer.upgrade().is_none(),
        "unadmitted ticket cannot keep Engine allocator alive"
    );
    let replacement = new_engine();
    let mut business = session(next.id());
    refused(
        replacement.prepare_session_with_identity(&next, &mut business, "replacement"),
        "different Engine",
    );
    no_business(&replacement);
}

#[test]
fn legacy_and_native_entries_share_the_same_checked_identity_space() {
    let engine = new_engine();
    let first = engine.mint_session_identity().unwrap();
    let mut legacy = session(first.id()); // Cannot claim a minted id via the old wrapper.
    engine.prepare_session(&mut legacy, "legacy").unwrap();
    assert_ne!(legacy.id, first.id().0);
    let second = engine.clone().mint_session_identity().unwrap();
    assert_ne!(first.id(), second.id());
    assert_ne!(legacy.id, second.id().0);
    let mut business = session(first.id());
    engine
        .prepare_session_with_identity(&first, &mut business, "native")
        .unwrap();
    let mut ids: Vec<_> = engine
        .active_sessions()
        .iter()
        .map(|item| item.id)
        .collect();
    ids.sort_unstable();
    let mut expected = vec![legacy.id, first.id().0];
    expected.sort_unstable();
    assert_eq!(ids, expected);
    for id in expected {
        engine
            .track_session(id)
            .finish(SessionOutcome::DirectRelayed)
            .unwrap();
    }
}

#[test]
fn approaching_max_never_wraps_either_entry_point_or_registers_a_failed_allocation() {
    let engine = new_engine();
    engine
        .next_session_id
        .store(u64::MAX - 2, Ordering::Relaxed);
    let identity = engine.mint_session_identity().unwrap();
    assert_eq!(identity.id(), SessionId(u64::MAX - 2));
    let mut legacy = session(SessionId(77));
    engine.prepare_session(&mut legacy, "legacy").unwrap();
    assert_eq!(legacy.id, u64::MAX - 1);
    assert!(matches!(
        engine.mint_session_identity(),
        Err(EngineError::AdmissionDenied { .. })
    ));
    let mut rejected = session(SessionId(88));
    let before = rejected.clone();
    refused(
        engine.prepare_session(&mut rejected, "exhausted"),
        "space exhausted",
    );
    assert_eq!(rejected, before);
    assert_eq!(engine.next_session_id.load(Ordering::Relaxed), u64::MAX);
    let mut business = session(identity.id());
    engine
        .prepare_session_with_identity(&identity, &mut business, "reserved")
        .unwrap();
    assert_eq!(engine.active_sessions().len(), 2);
    assert_eq!(events(&engine, event_type::FLOW_STARTED).len(), 2);
    for id in [business.id, legacy.id] {
        engine
            .track_session(id)
            .finish(SessionOutcome::DirectRelayed)
            .unwrap();
    }
}

#[test]
fn concurrent_attempts_of_the_same_ticket_register_exactly_one_real_session() {
    let engine = new_engine();
    let identity = engine.mint_session_identity().unwrap();
    let ready = std::sync::Barrier::new(2);
    let results = std::thread::scope(|scope| {
        let attempts: Vec<_> = (0..2)
            .map(|_| {
                scope.spawn(|| {
                    let mut business = session(identity.id());
                    ready.wait();
                    engine.prepare_session_with_identity(&identity, &mut business, "racing")
                })
            })
            .collect();
        attempts
            .into_iter()
            .map(|task| task.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(EngineError::AdmissionDenied { .. })))
            .count(),
        1
    );
    assert_eq!(engine.active_sessions().len(), 1);
    assert_eq!(events(&engine, event_type::FLOW_STARTED).len(), 1);
    engine
        .track_session(identity.id().0)
        .finish(SessionOutcome::DirectRelayed)
        .unwrap();
}
