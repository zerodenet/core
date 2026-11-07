use super::*;
use zero_api::EventSource;
fn make_engine(bytes: Option<usize>) -> Engine {
    let mut config =
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    config.runtime.event_subscriber_total_max_storage_bytes = bytes;
    Engine::new(config).unwrap()
}
#[test]
fn owned_initial_retains_the_original_book_without_using_queued_event_capacity() {
    let engine = make_engine(None);
    let handle = crate::EngineHandle::new(engine.clone());
    let subscriber = handle.subscribe(Default::default()).unwrap();
    let held = subscriber.try_recv_retained().unwrap();
    assert_eq!(held.event_type, zero_api::event_type::FLOW_SNAPSHOT);
    assert_eq!(subscriber.registration.original_pending_for_test(), 0);
    let sequence = held.sequence;
    let shared = held.into_shared();
    assert_eq!(shared.sequence, sequence);
    assert!(subscriber.try_recv_retained().is_none());
}
#[test]
fn failed_owned_initial_releases_original_guards_registration_and_storage_on_every_attempt() {
    // The exact production physical array still has 1024 slots. Establish its
    // actual prepaid book cost through the same subscriber, without an initial.
    let engine = make_engine(None);
    let live = EventSubscriber::subscribe_live(&engine, Default::default()).unwrap();
    let queue_bytes = live.registration.original_storage_for_test();
    drop(live);
    let engine = make_engine(Some(queue_bytes));
    let handle = crate::EngineHandle::new(engine.clone());
    for _ in 0..16 {
        assert_eq!(
            handle.subscribe(Default::default()).err().unwrap().code,
            zero_api::ApiErrorCode::Conflict
        );
        let live = EventSubscriber::subscribe_live(&engine, Default::default()).unwrap();
        assert!(live.try_recv_retained().is_none());
        drop(live);
    }
    // Uses the same history/membership guards again; no failed initial leaked
    // a lock or inserted a replay event to fabricate progress.
    let before = engine.latest_event_sequence();
    assert!(handle.subscribe_control_live(Default::default()).is_ok());
    assert_eq!(engine.latest_event_sequence(), before);
}
