use super::*;

#[test]
fn embedded_stale_domain_publication_restores_source_without_replacing_current_revision() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("zero.json");
    let original = config_with_outbounds(1);
    let original_bytes = serde_json::to_vec_pretty(&original).unwrap();
    std::fs::write(&path, &original_bytes).unwrap();
    let engine = Engine::new_with_config_path(original, &path).unwrap();
    let source = engine.configuration_source_owner();
    let stale = engine
        .prepare_configuration_snapshot(
            zero_runtime::configuration::PreparedConfiguration::prepare_owned(
                config_with_outbounds(2),
                source.clone(),
            )
            .unwrap(),
        )
        .unwrap();
    engine
        .reload_runtime_config(config_with_outbounds(3))
        .unwrap();
    let committed = engine.runtime_snapshot();
    let config = stale.config().clone();
    let error = source
        .commit_embedded(&config, true, "stale-embedded-domain", || {
            engine.publish_prepared_snapshot(stale)
        })
        .unwrap_err();
    assert!(matches!(
        error,
        zero_engine::EngineError::InvalidPlan { .. }
    ));
    assert!(Arc::ptr_eq(&engine.runtime_snapshot(), &committed));
    assert_eq!(engine.config_revision(), 2);
    assert_eq!(engine.config().outbounds.len(), 3);
    assert_eq!(std::fs::read(&path).unwrap(), original_bytes);
    source.confirm_original_io_closed().unwrap();
    assert_eq!(
        source.observation().disposition,
        zero_runtime::configuration::SourceDisposition::Restored
    );
    assert!(!directory.path().join("zero.json.zero-recovery").exists());
    assert!(!directory.path().join("zero.json.zero-last-good").exists());
}
