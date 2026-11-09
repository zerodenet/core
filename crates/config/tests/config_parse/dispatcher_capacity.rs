use zero_config::RuntimeConfig;
fn raw() -> serde_json::Value {
    serde_json::json!({
        "route":{"final":{"type":"direct"}},
        "api": {
            "event_sinks":[{"type":"jsonl","tag":"finite","path":"events.jsonl"}],
            "dead_letter_path":"dead.jsonl", "outbox_path":"outbox.jsonl"
        }
    })
}
#[test]
fn omitted_provider_limits_keep_the_existing_export_shape_and_configuration() {
    let config = RuntimeConfig::parse(&raw().to_string()).unwrap();
    let exported = serde_json::to_value(&config.api.dispatcher).unwrap();
    assert!(exported.get("max_worker_tasks").is_none());
    assert!(exported.get("max_open_originals").is_none());
}
#[test]
fn explicit_dispatcher_limits_validate_the_actual_layout_before_preparation() {
    let mut value = raw();
    // One JSONL pair + dead-letter pair + six actual outbox maintenance originals.
    value["api"]["dispatcher"] = serde_json::json!({"max_worker_tasks":4,"max_open_originals":10});
    RuntimeConfig::parse(&value.to_string()).unwrap();
    for (field, limit) in [
        ("max_worker_tasks", 3),
        ("max_open_originals", 7),
        ("max_open_originals", 8),
        ("max_open_originals", 9),
        ("max_worker_tasks", 0),
        ("max_open_originals", 0),
    ] {
        let mut invalid = value.clone();
        invalid["api"]["dispatcher"][field] = serde_json::json!(limit);
        let error = RuntimeConfig::parse(&invalid.to_string())
            .unwrap_err()
            .to_string();
        assert!(error.contains(field), "{field}: {error}");
    }
    let config = RuntimeConfig::parse(&value.to_string()).unwrap();
    let exported = serde_json::to_value(&config.api.dispatcher).unwrap();
    assert_eq!(exported["max_worker_tasks"], 4);
    assert_eq!(exported["max_open_originals"], 10);
}
