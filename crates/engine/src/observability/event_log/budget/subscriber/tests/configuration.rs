use super::*;
use zero_api::EventSource;
fn config() -> zero_config::RuntimeConfig {
    zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap()
}
fn filter() -> EventFilter {
    EventFilter {
        event_types: vec!["same.event".into()],
        ..Default::default()
    }
}
#[test]
fn ordinary_configuration_grows_same_receiver_and_rechecks_dequeued_originals_before_commit() {
    let mut cold = config();
    cold.runtime.event_subscriber_max_pending_events = Some(1);
    cold.runtime.event_subscriber_total_max_pending_events = Some(2);
    let engine = crate::Engine::new(cold.clone()).unwrap();
    let handle = crate::EngineHandle::new(engine.clone());
    let (subscriber, _) = handle.subscribe_control_live(filter()).unwrap();
    engine.emit_event((*event()).clone());
    let first = subscriber.try_recv_retained().unwrap();
    engine.emit_event((*event()).clone());
    assert!(subscriber.try_recv_retained().is_none());
    let mut grown = cold.clone();
    grown.runtime.event_subscriber_max_pending_events = Some(2);
    engine.reload_runtime_config(grown).unwrap();
    engine.emit_event((*event()).clone());
    let second = subscriber.try_recv_retained().unwrap();
    let revision = engine.config_revision();
    let error = engine.reload_runtime_config(cold.clone()).unwrap_err();
    assert!(
        matches!(error, crate::EngineError::Io(ref original) if original.kind() == io::ErrorKind::WouldBlock)
    );
    assert_eq!(engine.config_revision(), revision);
    assert_eq!(
        engine.config().runtime.event_subscriber_max_pending_events,
        Some(2)
    );
    assert_eq!(first.event_type, "same.event");
    drop(first);
    engine.reload_runtime_config(cold).unwrap();
    assert_eq!(second.event_type, "same.event");
    drop(second);
    engine.emit_event((*event()).clone());
    assert!(subscriber.try_recv_retained().is_some());
}
#[test]
fn actual_storage_shrink_waits_for_the_original_channel_and_can_grow_after_retirement() {
    let engine = crate::Engine::new(config()).unwrap();
    let handle = crate::EngineHandle::new(engine.clone());
    let (subscriber, _) = handle.subscribe_control_live(filter()).unwrap();
    let mut tiny = config();
    tiny.runtime.event_subscriber_total_max_storage_bytes = Some(1);
    assert!(
        matches!(engine.reload_runtime_config(tiny.clone()), Err(crate::EngineError::Io(ref original)) if original.kind() == io::ErrorKind::WouldBlock)
    );
    drop(subscriber); // Original receiver array and exact registration actually die.
    engine.reload_runtime_config(tiny).unwrap();
    assert_eq!(
        handle.subscribe(filter()).err().unwrap().code,
        zero_api::ApiErrorCode::Conflict
    );
    engine.reload_runtime_config(config()).unwrap();
    let (fresh, _) = handle.subscribe_control_live(filter()).unwrap();
    engine.emit_event((*event()).clone());
    assert_eq!(fresh.try_recv_retained().unwrap().event_type, "same.event");
}
