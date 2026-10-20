use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
struct ActualFile(PathBuf);
impl ActualFile {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "zero-native-file-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&path, b"four").unwrap();
        Self(path)
    }
}
impl Drop for ActualFile {
    fn drop(&mut self) {
        std::fs::remove_file(&self.0).unwrap();
    }
}

#[test]
fn actual_read_and_metadata_error_both_explicitly_close_the_original() {
    let file = ActualFile::new();
    let owner = PendingFileRead::new(file.0.clone(), 4).unwrap();
    assert_eq!(owner.run().unwrap(), b"four");
    assert!(matches!(
        owner.evidence().native,
        FileNativeEvidence::ConfirmedClosed
    ));
    assert!(owner.evidence().read_source.is_none());
    assert!(
        owner.run().is_err(),
        "a completed owner cannot acquire again"
    );
    let owner = PendingFileRead::new(file.0.clone(), 3).unwrap();
    let source = owner.run().unwrap_err();
    let evidence = owner.evidence();
    assert!(matches!(
        evidence.native,
        FileNativeEvidence::ConfirmedClosed
    ));
    assert!(Arc::ptr_eq(&source, evidence.read_source.as_ref().unwrap()));
    assert!(evidence.close_source.is_none());
}

#[tokio::test]
async fn real_post_open_worker_panic_leaves_the_original_until_join_then_native_close() {
    let file = ActualFile::new();
    let owner = Arc::new(PendingFileRead::new(file.0.clone(), 4).unwrap());
    let worker = owner.clone();
    let joined = tokio::task::spawn_blocking(move || {
        worker.run_with_after_open(|| panic!("post-open panic"))
    })
    .await;
    assert!(joined.unwrap_err().is_panic());
    assert!(matches!(owner.evidence().native, FileNativeEvidence::Owned));
    // Inspect the original retained object after the actual worker has exited.
    // No duplicate descriptor or task is created for this check.
    let state = owner.lock();
    let Native::Open(actual) = &state.native else {
        panic!("same original File must remain owned")
    };
    #[cfg(unix)]
    let original = {
        use std::os::{fd::AsRawFd, unix::fs::MetadataExt};
        let metadata = actual.metadata().unwrap();
        (actual.as_raw_fd(), metadata.dev(), metadata.ino())
    };
    #[cfg(windows)]
    let original = {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
        };
        let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
        assert_ne!(
            unsafe { GetFileInformationByHandle(actual.as_raw_handle(), info.as_mut_ptr()) },
            0
        );
        let info = unsafe { info.assume_init() };
        (
            actual.as_raw_handle(),
            (
                info.dwVolumeSerialNumber,
                info.nFileIndexHigh,
                info.nFileIndexLow,
            ),
        )
    };
    assert_eq!(actual.metadata().unwrap().len(), 4);
    drop(state);
    assert!(matches!(
        owner.close_after_join().native,
        FileNativeEvidence::ConfirmedClosed
    ));
    #[cfg(unix)]
    {
        let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
        let result = unsafe { libc::fstat(original.0, metadata.as_mut_ptr()) };
        if result == 0 {
            // Concurrent tests can reuse the number, never this unique file.
            let metadata = unsafe { metadata.assume_init() };
            assert_ne!(
                (metadata.st_dev as u64, metadata.st_ino as u64),
                (original.1, original.2)
            );
        } else {
            assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::EBADF));
        }
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Storage::FileSystem::{
            GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
        };
        let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
        if unsafe { GetFileInformationByHandle(original.0, info.as_mut_ptr()) } != 0 {
            let info = unsafe { info.assume_init() };
            assert_ne!(
                (
                    info.dwVolumeSerialNumber,
                    info.nFileIndexHigh,
                    info.nFileIndexLow
                ),
                original.1
            );
        } else {
            assert_eq!(
                io::Error::last_os_error().raw_os_error(),
                Some(windows_sys::Win32::Foundation::ERROR_INVALID_HANDLE as i32)
            );
        }
    }
    assert!(matches!(
        owner.close_after_join().native,
        FileNativeEvidence::ConfirmedClosed
    ));
}

#[test]
fn an_unwound_opening_window_is_permanently_unknown_and_retains_its_original_source() {
    let file = ActualFile::new();
    let owner = PendingFileRead::new(file.0.clone(), 4).unwrap();
    // Model precisely the pre-return open window. There is no invented close
    // acknowledgement: unlike KnownNoAcquisition, this state forbids retry.
    {
        let mut state = owner.lock();
        state.started = true;
        state.native = Native::Opening;
    }
    let first = owner.close_after_join();
    let FileNativeEvidence::Unknown(source) = first.native else {
        panic!("unproven open remains unknown")
    };
    for _ in 0..2 {
        let next = owner.close_after_join();
        let FileNativeEvidence::Unknown(next) = next.native else {
            panic!("no retry or ACK")
        };
        assert!(Arc::ptr_eq(&source, &next));
        assert!(owner.run().is_err());
    }
}
