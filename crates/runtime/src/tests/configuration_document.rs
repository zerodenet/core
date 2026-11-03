use super::*;

#[test]
fn initial_document_and_relative_resources_keep_the_same_actual_source() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("zero.json");
    std::fs::write(directory.path().join("domains.txt"), "example.test\n").unwrap();
    std::fs::write(&path, r#"{"route":{"rule_sets":[{"tag":"domains","type":"file","path":"domains.txt","format":"domain_list"}],"rules":[{"condition":{"type":"rule_set","tag":"domains"},"action":{"type":"direct"}}],"final":{"type":"reject"}}}"#).unwrap();
    let source = Arc::new(ConfigurationSource::new(&path));
    let config = source.load_initial_configuration(&path).unwrap();
    assert_eq!(config.source_dir(), Some(directory.path()));
    assert_eq!(source.io.original_observation(), (1, 1, false, false));
    let prepared =
        crate::configuration::PreparedConfiguration::prepare_startup(config, source.clone())
            .unwrap();
    assert!(Arc::ptr_eq(&prepared.source, &source));
    assert_eq!(prepared.source.path(), Some(path.as_path()));
    assert_eq!(source.io.original_observation(), (2, 2, false, false));
    zero_proxy::validate_prepared_configuration(&prepared).unwrap();
    // The standalone validator consumes the actual prepared projections rather
    // than reopening the initial document or its already prepared rule file.
    assert_eq!(source.io.original_observation(), (2, 2, false, false));
    assert_eq!(
        prepared
            .routing
            .router
            .decide_ref(&zero_core::Address::Domain("example.test".into()), None),
        &zero_router::RouteAction::Direct
    );
    // A load-only caller uses the same actual read/prepare path without gaining
    // an operator source path or any permission to persist to this document.
    let load_only = Arc::new(ConfigurationSource::default());
    let loaded = load_only.load_initial_configuration(&path).unwrap();
    assert!(load_only.path().is_none());
    assert_eq!(loaded.source_dir(), Some(directory.path()));
    assert_eq!(load_only.io.original_observation(), (1, 1, false, false));
}

#[test]
fn initial_document_errors_preserve_parse_and_read_shape_after_real_close() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("zero.json");
    let source = Arc::new(ConfigurationSource::new(&path));
    match source.load_initial_configuration(&path).unwrap_err() {
        ConfigError::ReadConfig {
            path: actual,
            source: error,
        } => {
            assert_eq!(actual, path.display().to_string());
            assert_eq!(error.kind(), io::ErrorKind::NotFound);
        }
        error => panic!("unexpected missing document error: {error}"),
    }
    assert_eq!(source.io.original_observation(), (0, 0, false, false));
    std::fs::write(&path, [0xff, 0xfe]).unwrap();
    match source.load_initial_configuration(&path).unwrap_err() {
        ConfigError::ReadConfig {
            path: actual,
            source: error,
        } => {
            assert_eq!(actual, path.display().to_string());
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        }
        error => panic!("unexpected invalid UTF-8 error: {error}"),
    }
    assert_eq!(source.io.original_observation(), (1, 1, false, false));
    std::fs::write(&path, b"{invalid json").unwrap();
    assert!(matches!(
        source.load_initial_configuration(&path).unwrap_err(),
        ConfigError::ParseConfig(_)
    ));
    assert_eq!(source.io.original_observation(), (2, 2, false, false));
    std::fs::write(&path, r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    source.load_initial_configuration(&path).unwrap();
    assert_eq!(source.io.original_observation(), (3, 3, false, false));
    source.confirm_original_io_closed().unwrap();
}

#[test]
fn early_document_reader_preserves_logging_fallback_inputs_and_can_start_after_clean_failure() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("zero.json");
    assert_eq!(
        ConfigurationSource::read_initial_document(&path)
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
    std::fs::write(&path, b"{invalid json").unwrap();
    assert_eq!(
        ConfigurationSource::read_initial_document(&path).unwrap(),
        "{invalid json"
    );
    // Logging continues to decide its own fallback from the unchanged raw text.
    std::fs::write(&path, r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    let source = Arc::new(ConfigurationSource::new(&path));
    source.load_initial_configuration(&path).unwrap();
    assert_eq!(source.io.original_observation(), (1, 1, false, false));
}
