use super::*;
#[test]
fn actual_old_file_larger_than_default_is_prepaid_and_recovery_uses_selected_file_limit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("large.json");
    let mut config = config();
    config.runtime.file_read_max_bytes = Some(10 * 1024 * 1024);
    config.runtime.control_max_in_flight_response_bytes = Some(20 * 1024 * 1024);
    let mut original = serde_json::to_vec_pretty(&config).unwrap();
    original.resize(8 * 1024 * 1024 + 17, b' ');
    std::fs::write(&path, &original).unwrap();
    let source = ConfigurationSource::new(&path);
    let book = ControlBudget::new(ControlBudgetLimits::from_runtime(&config.runtime));
    source.bind_persistence_budget(&book).unwrap();
    source
        .persist_ready_with_budget(
            &config,
            "actual-large",
            &book,
            None,
            config.runtime.effective_file_read_max_bytes(),
        )
        .unwrap();
    assert_eq!(
        book.usage().response_bytes,
        original.len() + record_storage("actual-large").unwrap()
    );
    assert_eq!(
        std::fs::read(files::sidecar(&path, ".zero-last-good")).unwrap(),
        original
    );
    assert_eq!(
        std::fs::read(&path).unwrap(),
        serde_json::to_vec_pretty(&config).unwrap()
    );
    closed(&source);
    ConfigurationSource::verify_recovery(&path).unwrap();
    let loaded = ConfigurationSource::load_document(&path).unwrap();
    assert_eq!(
        loaded.runtime.file_read_max_bytes,
        config.runtime.file_read_max_bytes
    );
    source.finalize_committed().unwrap();
    assert_eq!(book.usage().response_bytes, 0);
    closed(&source);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
}
