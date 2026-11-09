use super::*;
fn book(total: usize) -> Arc<ControlBudget> {
    ControlBudget::new(Limits { frame: 128, total }.budget_limits())
}
#[test]
fn original_cli_document_uses_total_for_raw_whitespace_and_keeps_capacity_until_data_frees() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("input.json");
    let mut raw = " ".repeat(256);
    raw.push_str(r#"{"route":{"final":{"type":"direct"}}}"#);
    std::fs::write(&path, &raw).unwrap();
    let source = Arc::new(ConfigurationSource::default());
    let book = book(65536);
    let data = load(&source, &path, &book).unwrap();
    assert_eq!(data.as_ref(), raw);
    assert!(raw.len() > book.limits().messages.ipc_frame_bytes);
    source.confirm_original_io_closed().unwrap();
    assert_eq!(book.usage().request_bytes, raw.len());
    let mut limits = book.limits();
    limits.messages.request_bytes_total = raw.len() - 1;
    assert!(book.prepare(limits).is_err());
    drop(data);
    assert_eq!(book.usage().request_bytes, 0);
    book.prepare(limits).unwrap().commit().unwrap();
    assert!(matches!(load(&source,&path,&book),Err(ref e) if e.kind()==io::ErrorKind::WouldBlock));
    source.confirm_original_io_closed().unwrap();
    assert_eq!(book.usage().request_bytes, 0);
}
#[test]
fn failed_utf8_keeps_the_original_paid_bytes_after_native_close_until_error_receiver_returns() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("invalid.json");
    std::fs::write(&path, [255, 254, 0]).unwrap();
    let source = Arc::new(ConfigurationSource::default());
    let book = book(65536);
    let error = load(&source, &path, &book).err().unwrap();
    source.confirm_original_io_closed().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(std::error::Error::source(&error).is_some());
    assert_eq!(book.usage().request_bytes, 3);
    drop(error);
    assert_eq!(book.usage().request_bytes, 0);
    std::fs::write(&path, b"{}").unwrap();
    let next = load(&source, &path, &book).unwrap();
    assert_eq!(next.as_ref(), "{}");
    drop(next);
    source.confirm_original_io_closed().unwrap();
    assert_eq!(book.usage().request_bytes, 0);
}

#[cfg(unix)]
#[test]
fn actual_directory_read_error_releases_credit_only_after_original_source_locks_and_close() {
    use std::{
        future::Future,
        sync::atomic::{AtomicBool, Ordering},
        task::{Context, Wake, Waker},
    };
    struct Check {
        source: Arc<ConfigurationSource>,
        woke: AtomicBool,
    }
    impl Wake for Check {
        fn wake(self: Arc<Self>) {
            self.source.confirm_original_io_closed().unwrap();
            self.woke.store(true, Ordering::Release);
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(ConfigurationSource::default());
    let book = book(65536);
    let check = Arc::new(Check {
        source: source.clone(),
        woke: AtomicBool::new(false),
    });
    let waker = Waker::from(check.clone());
    let mut changed = std::pin::pin!(book.capacity_changed());
    assert!(changed
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    let error = load(&source, dir.path(), &book).err().unwrap();
    assert!(error.raw_os_error().is_some());
    assert!(check.woke.load(Ordering::Acquire));
    source.confirm_original_io_closed().unwrap();
    assert_eq!(book.usage().request_bytes, 0);
}
