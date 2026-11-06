use super::*;
fn configured(directory: &Path) -> (Arc<ConfigurationSource>, RuntimeConfig) {
    let document = directory.join("zero.json");
    std::fs::write(
        &document,
        serde_json::to_vec(&serde_json::json!({
            "api":{"outbox_path":"outbox.jsonl","dead_letter_path":"dead.jsonl"},
            "route":{"final":{"type":"direct"}}
        }))
        .unwrap(),
    )
    .unwrap();
    let source = Arc::new(ConfigurationSource::new(&document));
    let config = source.load_initial_configuration(&document).unwrap();
    source.confirm_original_io_closed().unwrap();
    (source, config)
}
#[test]
fn same_loaded_source_inspects_both_snapshots_and_closes_real_files_and_sqlite_on_bad_records() {
    let directory = tempfile::tempdir().unwrap();
    let (source, config) = configured(directory.path());
    let outbox = directory.path().join("outbox.jsonl");
    let dead = directory.path().join("dead.jsonl");
    std::fs::write(
        &outbox,
        b"{\"op\":\"ack\",\"sink_tag\":\"same\",\"event_id\":\"one\"}\n",
    )
    .unwrap();
    std::fs::write(&dead, b"{\"event\":1}\n\n{\"event\":2}\n").unwrap();
    let (ready, faults) = inspect_persistent_state_with_source(&config, &source).into_parts();
    assert!(ready.compatible && faults.is_empty());
    assert_eq!(ready.files.len(), 2);
    assert_eq!(ready.files[0].records, Some(1));
    assert_eq!(ready.files[0].pending, Some(0));
    assert_eq!(ready.files[1].records, Some(2));
    source.confirm_original_io_closed().unwrap();
    assert!(!directory.path().join("outbox.jsonl.zero.lock").exists());
    std::fs::write(
        &outbox,
        b"not-json\n{\"op\":\"ack\",\"sink_tag\":\"same\",\"event_id\":\"two\"}\n",
    )
    .unwrap();
    let large = format!(
        "{{\"padding\":\"{}\",\"bad\":1e999}}\n",
        "x".repeat(crate::jsonl::BUFFER_BYTES + 1)
    );
    std::fs::write(&dead, &large).unwrap();
    let (invalid, faults) = inspect_persistent_state_with_source(&config, &source).into_parts();
    assert!(!invalid.compatible);
    assert!(invalid
        .files
        .iter()
        .all(|file| file.status == ConnectorStateStatus::Incompatible));
    assert!(invalid.files[0]
        .error
        .as_ref()
        .unwrap()
        .contains("malformed_middle"));
    assert!(invalid.files[1]
        .error
        .as_ref()
        .unwrap()
        .contains("invalid JSON"));
    assert_eq!(
        faults.len(),
        2,
        "independent actual decoder errors remain strong typed sources"
    );
    source.confirm_original_io_closed().unwrap();
    std::fs::write(
        &outbox,
        b"{\"op\":\"ack\",\"sink_tag\":\"same\",\"event_id\":\"three\"}\n",
    )
    .unwrap();
    std::fs::write(&dead, b"{}\n").unwrap();
    let (resumed, next_faults) =
        inspect_persistent_state_with_source(&config, &source).into_parts();
    assert!(
        resumed.compatible && next_faults.is_empty(),
        "closed decoder faults cannot veto another real inspection"
    );
    assert_eq!(
        faults.len(),
        2,
        "previous receiver survives the successful successor"
    );
    assert_eq!(std::fs::read(&dead).unwrap(), b"{}\n");
}
