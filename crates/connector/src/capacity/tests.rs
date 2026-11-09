use super::*;
#[test]
fn omitted_dispatcher_limits_preserve_the_existing_export_shape() {
    let value = serde_json::to_value(EventDispatcherConfig::default()).unwrap();
    assert!(value.get("max_worker_tasks").is_none());
    assert!(value.get("max_open_originals").is_none());
    let mut api = ApiConfig::default();
    api.dispatcher.max_worker_tasks = Some(1);
    api.dispatcher.max_open_originals = Some(1);
    assert!(
        DispatcherCapacity::validate(&api).is_ok(),
        "one actual cold initializer with no providers"
    );
    api.dispatcher.max_worker_tasks = Some(0);
    assert!(DispatcherCapacity::validate(&api).is_err());
}
#[test]
fn original_omissions_fit_configured_receivers_without_unlimited_admission() {
    let mut api = ApiConfig::default();
    for n in 0..80 {
        api.event_sinks.push(
            serde_json::from_value(serde_json::json!({
                "type":"jsonl", "tag":format!("output-{n}"), "path":format!("output-{n}.jsonl")
            }))
            .unwrap(),
        );
    }
    let (tasks, originals) = DispatcherCapacity::effective_limits(&api).unwrap();
    assert_eq!(tasks, 83);
    assert_eq!(originals, 160);
    assert!(DispatcherCapacity::validate(&api).is_ok());
    api.dispatcher.max_worker_tasks = Some(82);
    assert!(DispatcherCapacity::validate(&api).is_err());
    api.dispatcher.max_worker_tasks = Some(83);
    api.dispatcher.max_open_originals = Some(159);
    assert!(DispatcherCapacity::validate(&api).is_err());
}

#[test]
fn outbox_compaction_floor_retains_both_indexes_and_its_live_reader() {
    let mut api = ApiConfig::default();
    for n in 0..30 {
        api.event_sinks.push(
            serde_json::from_value(serde_json::json!({
                "type":"jsonl", "tag":format!("output-{n}"), "path":format!("output-{n}.jsonl")
            }))
            .unwrap(),
        );
    }
    api.outbox_path = Some("outbox.jsonl".into());
    assert_eq!(
        DispatcherCapacity::effective_limits(&api).unwrap(),
        (64, 66)
    );
    assert!(DispatcherCapacity::validate(&api).is_ok());
    // Explicit insufficient policy is refused BEFORE its first initializer/IO;
    // omitted old inputs adapt to the actual six-provider maintenance graph.
    for limit in [64, 65] {
        api.dispatcher.max_open_originals = Some(limit);
        assert!(DispatcherCapacity::validate(&api).is_err());
    }
    api.dispatcher.max_open_originals = Some(66);
    assert!(DispatcherCapacity::validate(&api).is_ok());
}
