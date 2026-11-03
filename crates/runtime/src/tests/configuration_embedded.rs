use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

fn config(mode: &str) -> RuntimeConfig {
    RuntimeConfig::parse(
        &serde_json::json!({
            "mode":{"type":mode},"route":{"final":{"type":"direct"}}
        })
        .to_string(),
    )
    .unwrap()
}
fn assert_originals_closed(source: &ConfigurationSource) {
    let (acquired, closed, retained, failed) = source.io.original_observation();
    assert!(acquired > 0);
    assert_eq!(acquired, closed);
    assert!(!retained && !failed);
    source.confirm_original_io_closed().unwrap();
}

#[test]
fn rejected_embedded_publication_restores_real_source_and_preserves_original_error() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("zero.json");
    let original = serde_json::to_vec_pretty(&config("rule")).unwrap();
    std::fs::write(&path, &original).unwrap();
    let source = Arc::new(ConfigurationSource::new(&path));
    let candidate = config("direct");
    let candidate_bytes = serde_json::to_vec_pretty(&candidate).unwrap();
    let error = source
        .commit_embedded(&candidate, true, "rejected-domain", || {
            assert_eq!(std::fs::read(&path).unwrap(), candidate_bytes);
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "actual domain rejected stale revision",
            ))
        })
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "actual domain rejected stale revision");
    assert_eq!(std::fs::read(&path).unwrap(), original);
    assert_eq!(
        source.observation().disposition,
        SourceDisposition::Restored
    );
    assert_originals_closed(&source);
    let installed = AtomicBool::new(false);
    source
        .commit_embedded(&candidate, true, "accepted-domain", || {
            installed.store(true, Ordering::Release);
            Ok::<(), io::Error>(())
        })
        .unwrap();
    assert!(installed.load(Ordering::Acquire));
    assert_eq!(std::fs::read(&path).unwrap(), candidate_bytes);
    assert_eq!(
        source.observation().disposition,
        SourceDisposition::Committed
    );
    assert_originals_closed(&source);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    std::fs::write(&path, &original).unwrap();
    ConfigurationSource::verify_recovery(&path).unwrap();
}

#[test]
fn embedded_write_failure_never_installs_and_retries_after_real_parent_repair() {
    let directory = tempfile::tempdir().unwrap();
    let parent = directory.path().join("source");
    std::fs::write(&parent, b"actual non-directory parent").unwrap();
    let path = parent.join("zero.json");
    let source = Arc::new(ConfigurationSource::new(&path));
    let installed = AtomicBool::new(false);
    let error = source
        .commit_embedded(&config("direct"), true, "invalid-parent", || {
            installed.store(true, Ordering::Release);
            Ok::<(), io::Error>(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotADirectory);
    assert!(!installed.load(Ordering::Acquire));
    assert_eq!(source.observation().successful_writes, 0);
    assert_eq!(
        std::fs::read(&parent).unwrap(),
        b"actual non-directory parent"
    );
    source.confirm_original_io_closed().unwrap();
    std::fs::remove_file(&parent).unwrap();
    std::fs::create_dir(&parent).unwrap();
    source
        .commit_embedded(&config("direct"), true, "repaired-parent", || {
            installed.store(true, Ordering::Release);
            Ok::<(), io::Error>(())
        })
        .unwrap();
    assert!(installed.load(Ordering::Acquire));
    assert_originals_closed(&source);
    assert_eq!(source.observation().successful_writes, 1);
}

#[cfg(unix)]
#[test]
fn embedded_unknown_namespace_blocks_publication_until_original_recovery_confirms() {
    let directory = tempfile::tempdir().unwrap();
    let parent = directory.path().join("source");
    let moved = directory.path().join("retained-source");
    std::fs::create_dir(&parent).unwrap();
    let path = parent.join("zero.json");
    let original = serde_json::to_vec_pretty(&config("rule")).unwrap();
    std::fs::write(&path, &original).unwrap();
    let obstruction = parent.clone();
    let retained = moved.clone();
    super::super::tests::fail_after_rename(path.clone(), move || {
        std::fs::rename(&obstruction, &retained).unwrap();
        std::fs::write(&obstruction, b"actual parent obstruction").unwrap();
    });
    let source = Arc::new(ConfigurationSource::new(&path));
    let installed = AtomicBool::new(false);
    let error = source
        .commit_embedded(&config("direct"), true, "unknown-namespace", || {
            installed.store(true, Ordering::Release);
            Ok::<(), io::Error>(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotADirectory);
    assert!(!installed.load(Ordering::Acquire));
    assert_eq!(source.observation().disposition, SourceDisposition::Unknown);
    let (acquired, closed, retained, failed) = source.io.original_observation();
    assert!(acquired > 0 && acquired == closed && !retained && !failed);
    // Native File close alone cannot resolve the actual source namespace.
    assert!(source
        .commit_embedded(&config("direct"), false, "blocked-runtime", || {
            installed.store(true, Ordering::Release);
            Ok::<(), io::Error>(())
        })
        .is_err());
    assert!(!installed.load(Ordering::Acquire));
    std::fs::remove_file(&parent).unwrap();
    std::fs::rename(&moved, &parent).unwrap();
    source.rollback_pending().unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), original);
    source
        .commit_embedded(&config("direct"), true, "confirmed-recovery", || {
            installed.store(true, Ordering::Release);
            Ok::<(), io::Error>(())
        })
        .unwrap();
    assert!(installed.load(Ordering::Acquire));
    assert_originals_closed(&source);
}

#[test]
fn concurrent_embedded_publications_use_the_same_source_and_complete_each_original() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("zero.json");
    std::fs::write(&path, serde_json::to_vec_pretty(&config("rule")).unwrap()).unwrap();
    let source = Arc::new(ConfigurationSource::new(&path));
    let first_completed = Arc::new(AtomicBool::new(false));
    let (entered, entering) = std::sync::mpsc::channel();
    let (release, releasing) = std::sync::mpsc::channel();
    let first_source = source.clone();
    let first_committed = first_completed.clone();
    let first = std::thread::spawn(move || {
        first_source
            .commit_embedded(&config("direct"), true, "first-domain", || {
                entered.send(()).unwrap();
                releasing.recv().unwrap();
                first_committed.store(true, Ordering::Release);
                Ok::<(), io::Error>(())
            })
            .unwrap();
    });
    entering.recv().unwrap();
    let (started, starting) = std::sync::mpsc::channel();
    let second_source = source.clone();
    let second = std::thread::spawn(move || {
        started.send(()).unwrap();
        second_source
            .commit_embedded(&config("rule"), true, "second-domain", || {
                assert!(first_completed.load(Ordering::Acquire));
                Ok::<(), io::Error>(())
            })
            .unwrap();
    });
    starting.recv().unwrap();
    release.send(()).unwrap();
    first.join().unwrap();
    second.join().unwrap();
    assert_eq!(
        std::fs::read(&path).unwrap(),
        serde_json::to_vec_pretty(&config("rule")).unwrap()
    );
    assert_eq!(source.observation().successful_writes, 2);
    assert_originals_closed(&source);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
}
