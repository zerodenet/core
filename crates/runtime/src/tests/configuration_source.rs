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

#[test]
fn source_backup_record_candidate_and_parent_sync_consume_each_original_close() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.json");
    std::fs::write(&path, b"original bytes").unwrap();
    let source = ConfigurationSource::new(&path);
    source
        .persist_ready(&candidate(), "actual-native-source-files")
        .unwrap();
    let (acquired, closed, occupied, failure) = source.io.original_observation();
    #[cfg(unix)]
    assert_eq!(acquired, 9); // source read + four atomic files + directory originals
    #[cfg(not(unix))]
    assert_eq!(acquired, 5); // existing non-Unix source helper has no directory sync
    assert_eq!(acquired, closed);
    assert!(!occupied && !failure);
    source.confirm_original_io_closed().unwrap();
    source.finalize_committed().unwrap();
    let (final_acquired, final_closed, occupied, failure) = source.io.original_observation();
    assert!(final_acquired > acquired);
    assert_eq!(final_acquired, final_closed);
    assert!(!occupied && !failure);
}

#[test]
fn interrupted_real_file_operation_blocks_source_rollback_and_later_preparation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.json");
    let source = ConfigurationSource::new(&path);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = source.io.with_file(
            || std::fs::File::create(&path),
            |_original| -> io::Result<()> {
                panic!("actual original source operation interrupted")
            },
        );
    }))
    .is_err());
    assert_eq!(source.io.original_observation(), (1, 0, true, true));
    let observation = source.observation();
    assert_eq!(observation.disposition, SourceDisposition::Unknown);
    assert_eq!(observation.successful_writes, 0);
    assert_eq!(observation.writable, None);
    assert_eq!(
        observation.reason.as_deref(),
        Some("source_file_cleanup_unconfirmed")
    );
    assert!(source.confirm_original_io_closed().is_err());
    assert_eq!(
        source.ensure_available().unwrap_err().disposition,
        SourceDisposition::Unknown
    );
    assert_eq!(
        source.rollback_pending().unwrap_err().disposition,
        SourceDisposition::Unknown
    );
    assert_eq!(
        source
            .persist_ready(&candidate(), "replacement-must-not-open")
            .unwrap_err()
            .disposition,
        SourceDisposition::Unknown
    );
    assert!(super::super::PreparedRouting::prepare_owned(&candidate(), &source).is_err());
    assert_eq!(source.io.original_observation(), (1, 0, true, true));
}
