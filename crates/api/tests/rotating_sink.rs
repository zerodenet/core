use zero_api::{ApiEvent, EventSink, RotatingFileSink};

fn event(id: &str) -> zero_api::RawApiEvent {
    ApiEvent::new(id, "engine.warning", 1, serde_json::json!({"id":id}))
}

#[test]
fn single_rotating_sink_keeps_complete_json_records_within_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.jsonl");
    std::fs::write(&path, vec![b'x'; 2048]).unwrap();
    let sink = RotatingFileSink::new("test", &path, 1024, 1).unwrap();
    for index in 0..100 {
        sink.publish(&event(&index.to_string())).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.len() <= 1024);
        for line in bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
        {
            serde_json::from_slice::<zero_api::RawApiEvent>(line).unwrap();
        }
    }
    assert!(!path.with_extension("1").exists());
}

#[test]
fn oversized_sink_record_is_preserved_and_not_split_across_files() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.jsonl");
    let sink = RotatingFileSink::new("test", &path, 512, 2).unwrap();
    let mut large = event("large");
    large.payload = serde_json::json!({"padding":"x".repeat(2048)});
    sink.publish(&large).unwrap();
    sink.publish(&event("small")).unwrap();
    let previous: zero_api::RawApiEvent =
        serde_json::from_slice(&std::fs::read(path.with_extension("1")).unwrap()).unwrap();
    assert_eq!(previous, large);
    assert_eq!(
        serde_json::from_slice::<zero_api::RawApiEvent>(&std::fs::read(path).unwrap())
            .unwrap()
            .event_id,
        "small"
    );
}

#[test]
fn sink_rotation_error_does_not_claim_a_fresh_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.jsonl");
    let sink = RotatingFileSink::new("test", &path, 1, 2).unwrap();
    sink.publish(&event("first")).unwrap();
    let original = std::fs::read(&path).unwrap();
    std::fs::create_dir(path.with_extension("1")).unwrap();
    assert!(sink.publish(&event("second")).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), original);
    std::fs::remove_dir(path.with_extension("1")).unwrap();
    sink.publish(&event("second")).unwrap();
    assert_eq!(std::fs::read(path.with_extension("1")).unwrap(), original);
}
