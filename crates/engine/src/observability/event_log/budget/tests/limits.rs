use super::*;
use crate::{Engine, EngineError};
fn filter() -> EventFilter {
    EventFilter {
        event_types: vec![event_type::STATS_SCOPES_SAMPLED.into()],
        ..Default::default()
    }
}
fn config(history: usize, subscriber: usize) -> zero_config::RuntimeConfig {
    let mut config =
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    config.runtime.event_log_stats_max_bytes = Some(history);
    config.runtime.event_subscriber_stats_max_bytes = Some(subscriber);
    config
}
fn would_block(error: EngineError) {
    let EngineError::Io(error) = error else {
        panic!("original byte owner must report its actual capacity error");
    };
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
}
#[test]
fn same_original_history_and_queued_event_survive_rejected_byte_shrink() {
    let log = EngineEventLog::default();
    let (tx, rx) = std::sync::mpsc::sync_channel(2);
    let (_registration, pending) = log.test_subscriber(tx, filter(), 2).unwrap();
    log.push_external(event());
    let queued = rx.recv().unwrap();
    let original = queued.event.clone();
    let used = pending.load(Ordering::Relaxed);
    assert!(used > 1);
    assert_eq!(log.inner.lock().unwrap().stats_bytes, used);
    let before = log.latest_sequence();
    assert_eq!(
        log.configure_statistics_limits(used - 1, used + 1)
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        log.configure_statistics_limits(used + 1, used - 1)
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    {
        let history = log.inner.lock().unwrap();
        assert_eq!(history.stats_limit, HISTORY_STATS_BYTES);
        assert!(Arc::ptr_eq(history.back().unwrap(), &original));
    }
    assert_eq!(
        log.subscriber_stats_limit.load(Ordering::Relaxed),
        SUBSCRIBER_STATS_BYTES
    );
    assert_eq!(log.latest_sequence(), before);
    drop(queued);
    assert_eq!(pending.load(Ordering::Relaxed), 0);
    log.configure_statistics_limits(used, 1).unwrap();
    assert!(Arc::ptr_eq(
        log.inner.lock().unwrap().back().unwrap(),
        &original
    ));
    assert_eq!(log.subscribers.lock().unwrap()[0].stats_limit, 1);
}
#[test]
fn ordinary_engine_cold_limits_and_hot_grow_use_the_same_actual_history_and_subscriber() {
    let engine = Engine::new(config(1024, 1024)).unwrap();
    let (tx, rx) = std::sync::mpsc::sync_channel(4);
    let (_registration_1, _) = engine.test_subscribe_events(tx, filter(), 4).unwrap();
    engine.emit_event(event());
    assert!(engine.events_snapshot(&filter()).is_empty());
    assert!(rx.try_recv().is_err());
    let sequence = engine.latest_event_sequence();
    engine.reload_runtime_config(config(65536, 65536)).unwrap();
    engine.emit_event(event());
    let actual = rx.recv().unwrap();
    assert!(actual.event.sequence.unwrap() > sequence);
    assert_eq!(actual.event.config_revision, Some(engine.config_revision()));
    assert_eq!(engine.events_snapshot(&filter()).len(), 1);
    drop(actual);
}
#[test]
fn ordinary_reload_is_busy_until_the_original_subscription_returns_its_pending_bytes() {
    let engine = Engine::new(config(65536, 65536)).unwrap();
    let (tx, rx) = std::sync::mpsc::sync_channel(4);
    let (_registration, book) = engine.test_subscribe_events(tx, filter(), 4).unwrap();
    engine.emit_event(event());
    let actual = rx.recv().unwrap();
    let retained = actual.event.clone();
    let sequence = engine.latest_event_sequence();
    let revision = engine.config_revision();
    let target = config(65536, 1);
    would_block(engine.reload_runtime_config(target.clone()).unwrap_err());
    assert_eq!(engine.config_revision(), revision);
    assert_eq!(engine.latest_event_sequence(), sequence);
    assert_eq!(
        engine.config().runtime.event_subscriber_stats_max_bytes,
        Some(65536)
    );
    assert!(book.load(Ordering::Relaxed) > 1);
    assert_eq!(actual.event.sequence, retained.sequence);
    drop(actual);
    assert_eq!(book.load(Ordering::Relaxed), 0);
    engine.reload_runtime_config(target).unwrap();
    assert_eq!(engine.config_revision(), revision + 1);
    // The same old subscriber and a newly registered subscriber both see the
    // installed limit. Their original queues/history are not replaced.
    let (tx2, rx2) = std::sync::mpsc::sync_channel(1);
    let (_registration_2, _) = engine.test_subscribe_events(tx2, filter(), 1).unwrap();
    engine.emit_event(event());
    assert!(rx.try_recv().is_err() && rx2.try_recv().is_err());
    assert!(engine
        .events_snapshot(&filter())
        .iter()
        .any(|event| event.sequence == retained.sequence));
}
#[test]
fn final_publication_rechecks_bytes_added_after_pure_preparation_without_trimming_old_history() {
    let engine = Engine::new(config(65536, 65536)).unwrap();
    let (tx, rx) = std::sync::mpsc::sync_channel(2);
    let (_registration_3, _) = engine.test_subscribe_events(tx, filter(), 2).unwrap();
    let mut target = config(65536, 1);
    target.runtime.event_log_capacity = 1;
    let prepared = zero_runtime::configuration::PreparedConfiguration::prepare_owned(
        target,
        engine.configuration_source_owner(),
    )
    .unwrap();
    let prepared = engine.prepare_configuration_snapshot(prepared).unwrap();
    engine.emit_event(event());
    let actual = rx.recv().unwrap();
    let before: Vec<_> = engine
        .events_snapshot(&EventFilter::default())
        .into_iter()
        .map(|event| (event.event_id, event.sequence))
        .collect();
    let revision = engine.config_revision();
    would_block(engine.publish_prepared_snapshot(prepared).unwrap_err());
    assert_eq!(engine.config_revision(), revision);
    assert_eq!(
        engine.config().runtime.event_subscriber_stats_max_bytes,
        Some(65536)
    );
    assert_eq!(
        engine
            .events_snapshot(&EventFilter::default())
            .into_iter()
            .map(|event| (event.event_id, event.sequence))
            .collect::<Vec<_>>(),
        before
    );
    drop(actual);
}
