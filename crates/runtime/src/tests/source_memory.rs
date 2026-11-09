//! Real Source writes/compensation share the already existing Host book.
use super::*;
use crate::configuration::{ControlBudgetLimits, ControlCharge};
fn config() -> RuntimeConfig {
    RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap()
}
fn book(bytes: usize) -> Arc<ControlBudget> {
    let mut limits = ControlBudgetLimits::from_runtime(&Default::default());
    limits.messages.response_bytes_total = bytes;
    ControlBudget::new(limits)
}
fn closed(source: &ConfigurationSource) {
    let (opened, closed, retained, fault) = source.io.original_observation();
    assert_eq!(opened, closed);
    assert!(!retained && !fault);
    source.confirm_original_io_closed().unwrap();
}
#[test]
fn pretty_rejection_happens_before_any_original_or_write_and_no_kernel_fallback_exists() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("zero.json");
    let original = b"original bytes";
    std::fs::write(&path, original).unwrap();
    let source = ConfigurationSource::new(&path);
    let book = book(1);
    source.bind_persistence_budget(&book).unwrap();
    let error = source
        .persist_ready_with_budget(&config(), "capacity", &book, None, 1024)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(error.disposition, SourceDisposition::Unchanged);
    assert_eq!(source.io.original_observation(), (0, 0, false, false));
    assert_eq!(std::fs::read(&path).unwrap(), original);
    assert_eq!(book.usage().response_bytes, 0);
    assert_eq!(
        source
            .persist_ready(&config(), "no-fallback")
            .unwrap_err()
            .kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(source.io.original_observation(), (0, 0, false, false));
    let mut foreign_limits = book.limits();
    foreign_limits.messages.response_bytes_total = 1024 * 1024;
    let foreign = ControlBudget::new(foreign_limits);
    let foreign_candidate = foreign.prepare(foreign_limits).unwrap();
    let error = source
        .persist_ready_with_budget(
            &config(),
            "foreign-candidate",
            &book,
            Some(&foreign_candidate),
            1024,
        )
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(source.io.original_observation(), (0, 0, false, false));
    assert_eq!(book.usage().response_bytes, 0);
    foreign_candidate.retire().unwrap();
}
#[test]
fn previous_backing_holds_its_own_credit_until_actual_finalization() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("zero.json");
    let original = b"actual previous source";
    std::fs::write(&path, original).unwrap();
    let config = config();
    let pretty = serde_json::to_vec_pretty(&config).unwrap();
    let book = book(pretty.len() + original.len() + record_storage("actual-commit").unwrap());
    let source = ConfigurationSource::new(&path);
    source.bind_persistence_budget(&book).unwrap();
    source
        .persist_ready_with_budget(&config, "actual-commit", &book, None, 1024)
        .unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), pretty);
    assert_eq!(
        book.usage().response_bytes,
        original.len() + record_storage("actual-commit").unwrap()
    );
    closed(&source);
    let mut shrink = book.limits();
    shrink.messages.response_bytes_total = original.len() - 1;
    assert_eq!(
        book.prepare(shrink).err().unwrap().kind(),
        io::ErrorKind::WouldBlock
    );
    source.finalize_committed().unwrap();
    assert_eq!(book.usage().response_bytes, 0);
    closed(&source);
    book.prepare(shrink).unwrap().commit().unwrap();
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
}
#[test]
fn previous_file_limit_rejects_before_read_growth_and_preserves_the_original() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("zero.json");
    let original = b"larger than its actual old file bound";
    std::fs::write(&path, original).unwrap();
    let source = ConfigurationSource::new(&path);
    let book = book(1024 * 1024);
    source.bind_persistence_budget(&book).unwrap();
    let error = source
        .persist_ready_with_budget(&config(), "bounded-old", &book, None, original.len() - 1)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(error.disposition, SourceDisposition::Unchanged);
    assert_eq!(book.usage().response_bytes, 0);
    assert_eq!(std::fs::read(&path).unwrap(), original);
    closed(&source);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
}
#[cfg(unix)]
#[test]
fn grow_and_real_unknown_compensation_keep_the_same_book_debt_after_rollback() {
    let directory = tempfile::tempdir().unwrap();
    let parent = directory.path().join("deployment");
    let retained = directory.path().join("actual-deployment");
    std::fs::create_dir(&parent).unwrap();
    let path = parent.join("zero.json");
    let original = b"actual retained previous bytes";
    std::fs::write(&path, original).unwrap();
    let source = ConfigurationSource::new(&path);
    let book = book(1);
    source.bind_persistence_budget(&book).unwrap();
    let config = config();
    let mut target = book.limits();
    target.messages.response_bytes_total = serde_json::to_vec_pretty(&config).unwrap().len()
        + original.len()
        + record_storage("actual-grow").unwrap();
    let candidate = book.prepare(target).unwrap();
    assert!(book.reserve(ControlCharge::ResponseBytes(2)).is_err());
    let moved = retained.clone();
    let old = parent.clone();
    super::super::tests::fail_after_rename(path.clone(), move || {
        std::fs::rename(&old, &moved).unwrap();
        std::fs::write(&old, b"actual non-directory obstruction").unwrap();
    });
    let error = source
        .persist_ready_with_budget(&config, "actual-grow", &book, Some(&candidate), 1024)
        .unwrap_err();
    assert_eq!(error.disposition, SourceDisposition::Unknown);
    assert_eq!(
        book.usage().response_bytes,
        original.len() + record_storage("actual-grow").unwrap()
    );
    candidate.retire().unwrap();
    assert_eq!(book.limits().messages.response_bytes_total, 1);
    assert!(book.reserve(ControlCharge::ResponseBytes(1)).is_err());
    assert_eq!(
        source.rollback_pending().unwrap_err().disposition,
        SourceDisposition::Unknown
    );
    assert_eq!(
        book.usage().response_bytes,
        original.len() + record_storage("actual-grow").unwrap()
    );
    std::fs::remove_file(&parent).unwrap();
    std::fs::rename(&retained, &parent).unwrap();
    source.rollback_pending().unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), original);
    assert_eq!(
        source.observation().disposition,
        SourceDisposition::Restored
    );
    assert_eq!(book.usage().response_bytes, 0);
    closed(&source);
    let next = book.reserve(ControlCharge::ResponseBytes(1)).unwrap();
    drop(next);
}

#[test]
fn actual_embedded_source_rejects_its_combined_backing_before_install_or_write() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("embedded.json");
    let source = Arc::new(ConfigurationSource::new(&path));
    let mut config = config();
    let original = b"actual original";
    std::fs::write(&path, original).unwrap();
    config.runtime.control_max_in_flight_response_bytes = Some(1000);
    for _ in 0..4 {
        let length = serde_json::to_vec_pretty(&config).unwrap().len();
        config.runtime.control_max_in_flight_response_bytes = Some(length + original.len() - 1);
    }
    let length = serde_json::to_vec_pretty(&config).unwrap().len();
    let cap = config
        .runtime
        .effective_control_max_in_flight_response_bytes();
    assert!(length <= cap && length + original.len() > cap);
    let installed = std::cell::Cell::new(false);
    let error = source
        .commit_embedded(&config, true, "embedded-capacity", || {
            installed.set(true);
            Ok::<_, io::Error>(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert!(!installed.get());
    assert_eq!(std::fs::read(&path).unwrap(), original);
    closed(&source);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
}
#[test]
fn successful_candidate_growth_only_opens_old_callers_after_actual_metadata_commit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("grow.json");
    std::fs::write(&path, b"old").unwrap();
    let source = ConfigurationSource::new(&path);
    let book = book(1);
    source.bind_persistence_budget(&book).unwrap();
    let mut target = book.limits();
    target.messages.response_bytes_total = 1024 * 1024;
    let candidate = book.prepare(target).unwrap();
    source
        .persist_ready_with_budget(&config(), "same-grow", &book, Some(&candidate), 1024)
        .unwrap();
    assert_eq!(
        book.usage().response_bytes,
        3 + record_storage("same-grow").unwrap()
    );
    assert!(book.reserve(ControlCharge::ResponseBytes(1)).is_err());
    candidate.commit_deferred().unwrap();
    candidate.publish_committed().unwrap();
    let extra = book.reserve(ControlCharge::ResponseBytes(1)).unwrap();
    drop(extra);
    source.finalize_committed().unwrap();
    assert_eq!(book.usage().response_bytes, 0);
    closed(&source);
}

#[path = "source_memory/wake.rs"]
mod wake;

#[path = "source_memory/large.rs"]
mod large;

#[path = "source_memory/admission.rs"]
mod admission;
