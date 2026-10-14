use super::*;

#[test]
fn preflight_rejects_payloads_delivery_cannot_load_without_touching_the_journal() {
    for padding in [0, crate::jsonl::BUFFER_BYTES + 1] {
        for invalid in [
            "1e999".to_owned(),
            r#""\uD800""#.to_owned(),
            format!("{}0{}", "[".repeat(160), "]".repeat(160)),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("outbox.jsonl");
            let mut value = delivery("invalid-payload", 3);
            value.event.payload = serde_json::Value::String("REPLACE_PAYLOAD".into());
            let record = serde_json::to_string(&JournalRecord::Put {
                delivery: Box::new(value),
            })
            .unwrap();
            let record = record.replace(
                "\"REPLACE_PAYLOAD\"",
                &format!(
                    r#"{{"padding":"{}","invalid":{invalid}}}"#,
                    "x".repeat(padding)
                ),
            );
            assert!(serde_json::from_str::<JournalRecord>(&record).is_err());
            let bytes = format!("{record}\n");
            std::fs::write(&path, &bytes).unwrap();
            let mut config = zero_config::RuntimeConfig::parse(
                r#"{"inbounds":[],"outbounds":[],"route":{"rules":[],"final":{"type":"direct"}}}"#,
            )
            .unwrap();
            config.api.outbox_path = Some(path.display().to_string());
            let report = inspect_persistent_state(&config);
            assert!(!report.compatible);
            assert_eq!(report.files[0].status, ConnectorStateStatus::Incompatible);
            assert!(inspect_path(&path)
                .unwrap_err()
                .to_string()
                .contains("malformed_tail"));
            assert_eq!(std::fs::read(&path).unwrap(), bytes.as_bytes());
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);

            config.api.outbox_path = None;
            config.api.dead_letter_path = Some(path.display().to_string());
            let report = inspect_persistent_state(&config);
            assert!(!report.compatible);
            assert_eq!(std::fs::read(&path).unwrap(), bytes.as_bytes());
        }
    }
}
