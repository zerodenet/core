use super::validate_prepared_configuration;
use std::sync::Arc;
use zero_runtime::configuration::{ConfigurationSource, PreparedConfiguration};

#[test]
fn prepared_validation_reuses_resources_after_initial_documents_are_removed() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("zero.json");
    let rules = directory.path().join("domains.txt");
    std::fs::write(&rules, "example.test\n").unwrap();
    std::fs::write(&path, r#"{"route":{"rule_sets":[{"tag":"domains","type":"file","path":"domains.txt","format":"domain_list"}],"rules":[{"condition":{"type":"rule_set","tag":"domains"},"action":{"type":"direct"}}],"final":{"type":"reject"}}}"#).unwrap();
    let source = Arc::new(ConfigurationSource::new(&path));
    let config = source.load_initial_configuration(&path).unwrap();
    assert_eq!(config.source_dir(), Some(directory.path()));
    let prepared = PreparedConfiguration::prepare_startup(config, source.clone()).unwrap();
    assert!(Arc::ptr_eq(&prepared.source, &source));
    assert_eq!(prepared.source.path(), Some(path.as_path()));
    source.confirm_original_io_closed().unwrap();

    // A validator that reopened either document would fail. The actual routing
    // projection and source remain the once-prepared inputs after removal.
    std::fs::remove_file(&path).unwrap();
    std::fs::remove_file(&rules).unwrap();
    validate_prepared_configuration(&prepared).unwrap();
    assert!(Arc::ptr_eq(&prepared.source, &source));
    source.confirm_original_io_closed().unwrap();
    assert_eq!(
        prepared
            .routing
            .router
            .decide_ref(&zero_core::Address::Domain("example.test".into()), None),
        &zero_router::RouteAction::Direct
    );
}
