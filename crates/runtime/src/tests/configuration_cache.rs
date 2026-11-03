use super::*;
use std::{cell::RefCell, io::Write};
type InstallHook = (PathBuf, Box<dyn FnOnce()>);
thread_local! { static AFTER_INSTALL: RefCell<Option<InstallHook>> = const { RefCell::new(None) }; }
pub(super) fn after_install(path: &Path) {
    let hook = AFTER_INSTALL.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.as_ref().is_some_and(|(expected, _)| expected == path) {
            slot.take()
        } else {
            None
        }
    });
    if let Some((_, hook)) = hook {
        hook();
    }
}
fn config() -> RuntimeConfig {
    RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap()
}
fn confirmed(source: &ConfigurationSource) {
    let (acquired, closed, retained, failed) = source.io.original_observation();
    assert!(acquired > 0);
    assert_eq!(
        acquired, closed,
        "each actual File has its matching native close"
    );
    assert!(!retained);
    assert!(!failed);
}
#[test]
fn actual_cache_chunks_install_only_after_original_close_and_preserve_default_permissions() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("rules.txt");
    std::fs::write(&path, b"old rule\n").unwrap();
    let source = Arc::new(ConfigurationSource::default());
    let reference = directory.path().join("default-mode");
    source
        .io
        .with_file(
            || {
                std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&reference)
            },
            |_| Ok(()),
        )
        .unwrap();
    let writer = source.prepare_rule_set_cache(&path).unwrap();
    writer.write_chunk(b"new ").unwrap();
    writer.write_chunk(b"rule\n").unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"old rule\n");
    let (acquired, closed, retained, failed) = source.io.original_observation();
    assert_eq!(acquired, closed + 1);
    assert!(retained && !failed);
    assert!(crate::configuration::PreparedRouting::prepare_owned(&config(), &source).is_err());
    assert!(
        crate::configuration::PreparedConfiguration::prepare_startup(config(), source.clone())
            .is_err()
    );
    assert!(source.prepare_rule_set_cache(&path).is_err());
    // This source is healthy and externally retained. Independent kernels
    // remain free to prepare, including concurrent callers of the short gate.
    let workers = (0..4)
        .map(|_| {
            std::thread::spawn(|| {
                crate::configuration::PreparedConfiguration::prepare(config()).unwrap()
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(writer.finish().unwrap(), 9);
    assert_eq!(std::fs::read(&path).unwrap(), b"new rule\n");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            std::fs::metadata(&reference).unwrap().permissions().mode() & 0o777
        );
    }
    confirmed(&source);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 2);
    source.confirm_original_io_closed().unwrap();
    assert!(writer.write_chunk(b"late").is_err());
    assert_eq!(writer.finish().unwrap(), 9);
}
#[test]
fn cancelled_cache_borrow_keeps_the_same_original_until_retained_clone_aborts() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("rules.txt");
    std::fs::write(&path, b"old").unwrap();
    let source = Arc::new(ConfigurationSource::default());
    let writer = source.prepare_rule_set_cache(&path).unwrap();
    let root = writer.clone();
    writer.write_chunk(b"candidate").unwrap();
    drop(writer); // caller loss is not file close or release
    assert_eq!(source.io.original_observation(), (1, 0, true, false));
    assert_eq!(std::fs::read(&path).unwrap(), b"old");
    assert!(source.confirm_original_io_closed().is_err());
    assert!(source.prepare_rule_set_cache(&path).is_err());
    root.abort().unwrap();
    confirmed(&source);
    assert_eq!(std::fs::read(&path).unwrap(), b"old");
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    source.abort_rule_set_cache().unwrap(); // confirmed no pending, no new IO
    crate::configuration::PreparedRouting::prepare_owned(&config(), &source).unwrap();
}
#[cfg(unix)]
#[test]
fn actual_cache_directory_sync_failure_retains_namespace_until_explicit_abort_restores_old() {
    let directory = tempfile::tempdir().unwrap();
    let parent = directory.path().join("holder");
    std::fs::create_dir(&parent).unwrap();
    let moved = directory.path().join("moved");
    let path = parent.join("rules.txt");
    std::fs::write(&path, b"original").unwrap();
    let source = Arc::new(ConfigurationSource::default());
    let writer = source.prepare_rule_set_cache(&path).unwrap();
    writer.write_chunk(b"candidate").unwrap();
    let changed_parent = parent.clone();
    let moved_parent = moved.clone();
    AFTER_INSTALL.with(|hook| {
        *hook.borrow_mut() = Some((
            path.clone(),
            Box::new(move || {
                std::fs::rename(&changed_parent, &moved_parent).unwrap();
                std::fs::write(&changed_parent, b"actual parent is now a file").unwrap();
            }),
        ))
    });
    let error = writer.finish().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotADirectory);
    confirmed(&source); // actual native File close is separate from namespace recovery
    assert!(source.confirm_original_io_closed().is_err());
    assert!(writer.write_chunk(b"late").is_err());
    assert!(writer.abort().is_err());
    assert_eq!(
        std::fs::read(moved.join("rules.txt")).unwrap(),
        b"candidate"
    );
    std::fs::remove_file(&parent).unwrap();
    std::fs::rename(&moved, &parent).unwrap();
    writer.abort().unwrap();
    confirmed(&source);
    assert_eq!(std::fs::read(&path).unwrap(), b"original");
    assert_eq!(std::fs::read_dir(&parent).unwrap().count(), 1);
    source.confirm_original_io_closed().unwrap();
}
#[test]
fn abandoned_cache_view_is_recovered_from_the_same_source_without_drop_cleanup() {
    const CHILD: &str = "ZERO_CACHE_ORPHAN_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "configuration::source::cache::tests::abandoned_cache_view_is_recovered_from_the_same_source_without_drop_cleanup", "--nocapture"])
            .env(CHILD, "1").status().unwrap();
        assert!(result.success());
        return;
    }
    // Isolate this intentionally orphaned global debt from unrelated test
    // kernels. This is not serializing healthy production cache preparations.
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("rules.txt");
    let source = Arc::new(ConfigurationSource::default());
    let writer = source.prepare_rule_set_cache(&path).unwrap();
    writer.write_chunk(b"unpublished").unwrap();
    drop(writer);
    assert_eq!(source.io.original_observation(), (1, 0, true, false));
    assert!(crate::configuration::PreparedConfiguration::prepare(config()).is_err());
    source.abort_rule_set_cache().unwrap();
    confirmed(&source);
    assert!(!path.exists());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    crate::configuration::PreparedConfiguration::prepare(config()).unwrap();
}
#[test]
fn cache_cleanup_never_clears_a_foreign_original_unknown_or_allows_kernel_build() {
    const CHILD: &str = "ZERO_CACHE_FOREIGN_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "configuration::source::cache::tests::cache_cleanup_never_clears_a_foreign_original_unknown_or_allows_kernel_build", "--nocapture"])
            .env(CHILD, "1").status().unwrap();
        assert!(result.success());
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let cache = directory.path().join("rules.txt");
    let source = Arc::new(ConfigurationSource::default());
    let writer = source.prepare_rule_set_cache(&cache).unwrap();
    writer.write_chunk(b"complete").unwrap();
    let foreign_path = directory.path().join("foreign");
    let foreign = Arc::new(ConfigurationSource::default());
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = super::super::super::startup::with_source(foreign.clone(), || {
            foreign.io.with_file(
                || std::fs::File::create(&foreign_path),
                |file| -> io::Result<()> {
                    file.write_all(b"real pending original")?;
                    panic!("actual cold material operation interrupted while retaining its file")
                },
            )
        });
    }))
    .is_err());
    assert!(writer.finish().is_err());
    confirmed(&source); // this cache's own originals actually closed
    assert!(writer.abort().is_err()); // no authority over the foreign debt
    assert!(source.confirm_original_io_closed().is_err());
    assert!(crate::configuration::PreparedRouting::prepare_owned(
        &config(),
        &Arc::new(ConfigurationSource::default())
    )
    .is_err());
    assert!(crate::configuration::confirm_preparation_io_closed().is_err());
    assert!(Arc::new(ConfigurationSource::default())
        .persist_ready(&config(), "blocked-cold-debt")
        .is_err());
    assert_eq!(foreign.io.original_observation(), (1, 0, true, true));
}
