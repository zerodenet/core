//! Faults mutate the real filesystem after the actual rename. They never
//! synthesize a successful write, native close receipt, or cleanup outcome.
use super::*;
use std::cell::RefCell;
type RenameHook = (PathBuf, Box<dyn FnOnce()>);
thread_local! {
    static AFTER_RENAME: RefCell<Option<RenameHook>> = const { RefCell::new(None) };
}
pub(super) fn after_rename(path: &Path) {
    let hook = AFTER_RENAME.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.as_ref().is_some_and(|(target, _)| target == path) {
            slot.take()
        } else {
            None
        }
    });
    if let Some((_, hook)) = hook {
        hook();
    }
}
fn fail_after_rename(path: PathBuf, hook: impl FnOnce() + 'static) {
    AFTER_RENAME.with(|slot| assert!(slot.borrow_mut().replace((path, Box::new(hook))).is_none()));
}
fn candidate() -> RuntimeConfig {
    RuntimeConfig::parse(r#"{"route":{"final":{"type":"reject"}}}"#).unwrap()
}

#[test]
fn absent_original_is_physically_removed_when_final_record_write_fails() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("zero.json");
    let record = files::sidecar(&path, ".zero-recovery");
    let obstructed = record.clone();
    fail_after_rename(path.clone(), move || {
        std::fs::remove_file(&obstructed).unwrap();
        std::fs::create_dir(&obstructed).unwrap();
    });
    let source = ConfigurationSource::new(path.clone());
    let error = source
        .persist_ready(&candidate(), "original-absent-record-failure")
        .unwrap_err();
    assert_eq!(error.disposition, SourceDisposition::Restored);
    assert_eq!(source.observation().successful_writes, 0);
    assert!(!path.exists());
    assert!(
        matches!(std::fs::metadata(&path), Err(error) if error.kind() == io::ErrorKind::NotFound)
    );
    std::fs::remove_dir(record).unwrap();
    source.rollback_pending().unwrap();
    source.ensure_available().unwrap();
}

#[cfg(unix)]
#[test]
fn actual_directory_sync_failure_retains_unknown_until_original_absence_is_confirmed() {
    let directory = tempfile::tempdir().unwrap();
    let original_directory = directory.path().join("deployment");
    let retained_directory = directory.path().join("retained-deployment");
    std::fs::create_dir(&original_directory).unwrap();
    let path = original_directory.join("zero.json");
    let moved = retained_directory.clone();
    let original = original_directory.clone();
    fail_after_rename(path.clone(), move || {
        std::fs::rename(&original, &moved).unwrap();
        std::fs::write(&original, b"actual parent-path obstruction").unwrap();
    });
    let source = ConfigurationSource::new(path.clone());
    let error = source
        .persist_ready(&candidate(), "actual-sync-failure")
        .unwrap_err();
    assert_eq!(error.disposition, SourceDisposition::Unknown);
    assert_eq!(source.observation().disposition, SourceDisposition::Unknown);
    assert_eq!(source.observation().successful_writes, 0);
    assert!(retained_directory.join("zero.json").is_file());
    assert_eq!(
        source.ensure_available().unwrap_err().disposition,
        SourceDisposition::Unknown
    );
    assert_eq!(
        source.rollback_pending().unwrap_err().disposition,
        SourceDisposition::Unknown
    );
    std::fs::remove_file(&original_directory).unwrap();
    std::fs::rename(&retained_directory, &original_directory).unwrap();
    source.rollback_pending().unwrap();
    assert_eq!(
        source.observation().disposition,
        SourceDisposition::Restored
    );
    assert!(
        matches!(std::fs::metadata(&path), Err(error) if error.kind() == io::ErrorKind::NotFound)
    );
    source.ensure_available().unwrap();
}

#[test]
fn actual_commit_finalization_allows_a_later_operator_edit_and_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("zero.json");
    std::fs::write(&path, b"{}").unwrap();
    let source = ConfigurationSource::new(path.clone());
    source
        .persist_ready(&candidate(), "completed-original")
        .unwrap();
    source.finalize_committed().unwrap();
    assert_eq!(
        source.observation().disposition,
        SourceDisposition::Committed
    );
    assert!(!files::sidecar(&path, ".zero-recovery").exists());
    assert!(!files::sidecar(&path, ".zero-last-good").exists());
    std::fs::write(&path, b"{\"route\":{\"final\":{\"type\":\"direct\"}}}").unwrap();
    ConfigurationSource::verify_recovery(&path).unwrap();
    let restarted = RuntimeConfig::load_from_path(path).unwrap();
    assert!(matches!(
        restarted.route.final_action,
        zero_config::RouteActionConfig::Direct
    ));
}
