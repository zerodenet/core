use super::*;
fn oracle(raw: &str) -> Option<zero_config::LogConfig> {
    let original: serde_json::Value = serde_json::from_str(raw).ok()?;
    serde_json::from_value(original.get("runtime")?.get("log")?.clone()).ok()
}
fn magic(raw: &str) -> String {
    serde_json::json!({"$serde_json::private::RawValue": raw}).to_string()
}
#[test]
fn early_logging_uses_the_original_serde_value_oracle_without_a_whole_document_tree() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("input.json");
    let level = r#"{"level":"debug"}"#;
    let runtime = format!(r#"{{"log":{level}}}"#);
    let document = format!(r#"{{"runtime":{runtime}}}"#);
    let long = "\\u0061".repeat(20_000);
    let cases = vec![
        document.clone(),
        magic(&document),
        format!(r#"{{"runtime":{}}}"#, magic(&runtime)),
        format!(r#"{{"runtime":{{"log":{}}}}}"#, magic(level)),
        format!(r#"{{"runtime":{runtime},"runtime":{{"log":{{"level":"warn"}}}}}}"#),
        r#"{"runtime":{"log":{"level":"debug","level":"error"}}}"#.into(),
        r#"{"runtime":{"file_read_max_bytes":"invalid-first","file_read_max_bytes":null,"log":{"level":"debug"}}}"#.into(),
        r#"{"runtime":{"file_read_max_bytes":{"ordinary":1},"file_read_max_bytes":null,"log":{"level":"debug"}}}"#.into(),
        r#"{"runtime":{"file_read_max_bytes":"bad"},"runtime":{"log":{"level":"debug"}}}"#.into(),
        format!(r#"{{"runtime":{},"runtime":{runtime}}}"#, magic(r#"{"file_read_max_bytes":"bad"}"#)),
        r#"{"runtime":{"log":null}}"#.into(),
        r#"{"runtime":null}"#.into(),
        r#"{"runtime":{"log":{"unknown":1}}}"#.into(),
        r#"{"other":{"$serde_json::private::RawValue":"{broken"},"runtime":{"log":{"level":"debug"}}}"#.into(),
        r#"{"other":{"$serde_json::private::RawValue":"{}","extra":1},"runtime":{"log":{"level":"debug"}}}"#.into(),
        r#"{"other":{"a":1,"$serde_json::private::RawValue":"{broken"},"runtime":{"log":{"level":"debug"}}}"#.into(),
        r#"{"other":1e400,"runtime":{"log":{"level":"debug"}}}"#.into(),
        format!(r#"{{"{long}":0,"runtime":{runtime}}}"#),
        "{broken".into(),
        "null".into(),
    ];
    for raw in cases {
        let expected = oracle(&raw);
        std::fs::write(&path, raw.as_bytes()).unwrap();
        let actual = ConfigurationSource::read_initial_log_config(&path)
            .ok()
            .flatten();
        assert_eq!(
            actual,
            expected,
            "original serde Value result must stay identical: {}",
            &raw[..raw.len().min(160)]
        );
    }
}
#[test]
fn explicit_late_file_budget_keeps_a_legal_document_above_the_old_default_loadable() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("large.json");
    let level = "x".repeat(8 * 1024 * 1024 + 17);
    // No protocol-private fact is inspected. This is valid configuration data;
    // the actual file limit appears after the long value, before any read cap.
    let raw =
        format!(r#"{{"runtime":{{"log":{{"level":"{level}"}},"file_read_max_bytes":16777216}}}}"#);
    let expected =
        RuntimeConfig::parse_with_source_dir(&raw, Some(directory.path().to_path_buf())).unwrap();
    std::fs::write(&path, &raw).unwrap();
    let source = Arc::new(ConfigurationSource::new(&path));
    let actual = source.load_initial_configuration(&path).unwrap();
    assert_eq!(actual.runtime.log, expected.runtime.log);
    assert_eq!(actual.runtime.file_read_max_bytes, Some(16 * 1024 * 1024));
    assert_eq!(source.io.original_observation(), (1, 1, false, false));
    let projected = ConfigurationSource::read_initial_log_config(&path)
        .unwrap()
        .unwrap();
    assert_eq!(projected, expected.runtime.log);
    source.confirm_original_io_closed().unwrap();
}
#[test]
fn initial_input_bounds_and_invalid_facts_close_the_actual_file_before_next_attempt() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("input.json");
    let source = Arc::new(ConfigurationSource::new(&path));
    std::fs::write(&path, r#"{"runtime":{"file_read_max_bytes":1}}"#).unwrap();
    let error = source.load_initial_configuration(&path).unwrap_err();
    assert!(matches!(error, ConfigError::ReadConfig { .. }));
    assert_eq!(source.io.original_observation(), (1, 1, false, false));
    for raw in [
        r#"{"runtime":{"file_read_max_bytes":184467440737095516160000}}"#,
        r#"{"runtime":{"file_read_max_bytes":"invalid"}}"#,
        "{broken",
    ] {
        std::fs::write(&path, raw).unwrap();
        assert!(RuntimeConfig::parse(raw).is_err());
        assert!(matches!(
            source.load_initial_configuration(&path),
            Err(ConfigError::ParseConfig(_))
        ));
    }
    assert_eq!(source.io.original_observation(), (4, 4, false, false));
    std::fs::write(&path, [0xff, 0xfe]).unwrap();
    match source.load_initial_configuration(&path).unwrap_err() {
        ConfigError::ReadConfig { source, .. } => {
            assert_eq!(source.kind(), io::ErrorKind::InvalidData)
        }
        error => panic!("UTF-8 must remain the original read boundary: {error}"),
    }
    std::fs::write(&path, r#"{"runtime":{"file_read_max_bytes":null}}"#).unwrap();
    source.load_initial_configuration(&path).unwrap();
    assert_eq!(source.io.original_observation(), (6, 6, false, false));
    source.confirm_original_io_closed().unwrap();
}
#[test]
fn early_log_storage_uses_the_actual_configured_cold_envelope_before_materializing_rows() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("input.json");
    let raw = r#"{"runtime":{"control_max_in_flight_request_bytes":1,"log":{"files":[{"path":"never-opened.log"}]}}}"#;
    assert!(oracle(raw).is_some());
    std::fs::write(&path, raw).unwrap();
    let error = ConfigurationSource::read_initial_log_config(&path).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    // The bound is configurable on the next ordinary read, not a permanent
    // admission latch, and this projection never opens any logging output.
    let raw = raw.replace("request_bytes\":1", "request_bytes\":1048576");
    std::fs::write(&path, &raw).unwrap();
    assert_eq!(
        ConfigurationSource::read_initial_log_config(&path).unwrap(),
        oracle(&raw)
    );
    assert!(!directory.path().join("never-opened.log").exists());
}
