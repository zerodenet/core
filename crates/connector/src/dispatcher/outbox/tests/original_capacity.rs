//! Same production registry/resources/outbox: omitted quotas must maintain all facts.
use super::*;
use std::sync::Arc;
#[test]
fn thirty_existing_jsonl_sinks_and_outbox_compact_at_the_actual_finite_floor() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("outbox.jsonl");
    let mut api = zero_config::ApiConfig::default();
    for n in 0..30 {
        api.event_sinks.push(
            serde_json::from_value(serde_json::json!({
                "type":"jsonl", "tag":format!("output-{n}"),
                "path":directory.path().join(format!("output-{n}.jsonl"))
            }))
            .unwrap(),
        );
    }
    api.outbox_path = Some(path.to_str().unwrap().into());
    assert!(api.dispatcher.max_open_originals.is_none());
    crate::capacity::DispatcherCapacity::validate(&api).unwrap();
    let capacity = Arc::new(crate::capacity::DispatcherCapacity::from_api(&api));
    let resources = crate::resources::DispatcherResources::with_capacity(capacity.clone());
    let children =
        Arc::new(crate::dispatcher::children::DispatcherChildren::with_capacity(capacity));
    // Real JSONL files and their real lease FDs stay open throughout. This
    // standalone fixture performs NO networking; all these sinks are JSONL.
    let sinks = crate::registry::build_event_sinks(
        &api,
        None,
        &crate::network::EventDispatcherNetwork::system(),
        &resources,
        children,
    )
    .unwrap();
    assert_eq!(sinks.len(), 30);
    assert_eq!(resources.original_file_count_for_test(), 60);
    let mut outbox =
        DeliveryOutbox::open_owned(resources.clone(), path.to_str().unwrap(), None, 0, 0).unwrap();
    outbox.put(&delivery("unACKed-stable", 7)).unwrap();
    for attempt in 0..1030 {
        outbox.put(&delivery("unACKed-retry", attempt)).unwrap();
    }
    assert!(
        outbox.journal_records < 1024,
        "actual compaction reached its maintenance graph"
    );
    assert_eq!(outbox.pending_counts().unwrap()["receiver"], 2);
    let pending = outbox.load_pending_excluding(&HashSet::new(), 3).unwrap();
    assert_eq!(pending.len(), 2);
    let stable = pending
        .iter()
        .find(|event| event.event.event_id == "unACKed-stable")
        .unwrap();
    let latest = pending
        .iter()
        .find(|event| event.event.event_id == "unACKed-retry")
        .unwrap();
    assert_eq!(stable.attempts, 7);
    assert_eq!(stable.event.payload, serde_json::json!({"attempt":7}));
    assert_eq!(stable.message.as_deref(), Some("retry 7"));
    assert_eq!(latest.attempts, 1029);
    assert_eq!(latest.event.payload, serde_json::json!({"attempt":1029}));
    assert_eq!(latest.message.as_deref(), Some("retry 1029"));
    assert!(outbox.recovery_status().is_none());
    drop(pending);
    drop(outbox);
    assert!(
        !resources.retirement_complete(),
        "returning file views is not native close"
    );
    // Reopening uses this SAME actual inventory. Its reserve path must close
    // returned originals and consume their matching receipts before new IO.
    let reopened =
        DeliveryOutbox::open_owned(resources.clone(), path.to_str().unwrap(), None, 0, 0).unwrap();
    let durable = reopened.load_pending_excluding(&HashSet::new(), 3).unwrap();
    assert_eq!(durable.len(), 2);
    assert!(durable
        .iter()
        .any(|row| row.event.event_id == "unACKed-stable" && row.attempts == 7));
    assert!(durable
        .iter()
        .any(|row| row.event.event_id == "unACKed-retry" && row.attempts == 1029));
    drop(durable);
    drop(reopened);
    drop(sinks);
    resources.close().unwrap(); // actual OriginalFile matching close + SQLite/scratch retirement
    assert!(resources.retirement_complete());
    assert_eq!(resources.original_file_count_for_test(), 0);
}
