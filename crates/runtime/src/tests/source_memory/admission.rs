//! A retained candidate does not reopen a stopped actual Host.
use super::*;

#[test]
fn closed_host_rejects_candidate_backing_before_file_acquisition() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("zero.json");
    let original = b"original configuration";
    std::fs::write(&path, original).unwrap();
    let source = ConfigurationSource::new(&path);
    let book = book(1);
    source.bind_persistence_budget(&book).unwrap();
    let mut limits = book.limits();
    limits.messages.response_bytes_total = 1024 * 1024;
    let candidate = book.prepare(limits).unwrap();
    book.close_admission();

    let error = source
        .persist_ready_with_budget(&config(), "closed-host", &book, Some(&candidate), 1024)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(error.disposition, SourceDisposition::Unchanged);
    assert_eq!(source.io.original_observation(), (0, 0, false, false));
    assert_eq!(std::fs::read(&path).unwrap(), original);
    assert_eq!(book.usage().response_bytes, 0);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    candidate.retire().unwrap();
    assert_eq!(
        book.reserve(ControlCharge::ResponseBytes(1))
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
}
