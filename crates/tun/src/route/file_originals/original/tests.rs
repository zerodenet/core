use super::*;
use crate::packet_memory::PacketReturnTotals;
use std::sync::Arc;
#[test]
fn same_original_close_unknown_never_retries_and_both_causes_survive() {
    let root = Arc::new(PacketReturnTotals::default());
    let view = root.driver_metadata_view();
    let baseline = root.driver_metadata().occupied;
    let faults = RouteFileFaults::prepare(&view).unwrap();
    let attempt = faults.begin(&view).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("actual-lock");
    let file = File::create(path).unwrap();
    let mut original = TrackedFile::pending(faults.clone(), attempt.clone());
    original.install(file);
    let source = io::Error::new(io::ErrorKind::ConnectionRefused, "actual preparation cause");
    let same_payload = source.get_ref().unwrap() as *const _;
    faults.retain(&attempt, 0, source);
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        // Deliberate foreign invalidation of THIS original. No subsequent
        // native acquisition occurs before its one close reports EBADF.
        assert_eq!(
            unsafe { libc::close(original.file().unwrap().as_raw_fd()) },
            0
        );
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        assert_ne!(
            unsafe {
                windows_sys::Win32::Foundation::CloseHandle(
                    original.file().unwrap().as_raw_handle(),
                )
            },
            0
        );
    }
    let first = original.close().unwrap_err();
    assert!(!original.closed());
    assert_eq!(original.close_calls, 1);
    assert_eq!(
        original.close().unwrap_err().raw_os_error(),
        first.raw_os_error()
    );
    assert_eq!(
        original.close_calls, 1,
        "unknown result cannot execute a second close"
    );
    let diagnostic = faults.diagnostic();
    let received = diagnostic
        .as_error()
        .source()
        .unwrap()
        .downcast_ref::<io::Error>()
        .unwrap();
    assert!(std::ptr::eq(
        received.get_ref().unwrap() as *const _,
        same_payload
    ));
    let text = diagnostic.as_error().to_string();
    assert!(text.contains("actual preparation cause") && text.contains("phase 3"));
    drop(original);
    drop(attempt);
    drop(faults);
    assert_ne!(
        root.driver_metadata().occupied,
        baseline,
        "actual diagnostic still owns all cause headers"
    );
    drop(diagnostic);
    assert_eq!(root.driver_metadata().occupied, baseline);
}
