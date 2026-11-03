use super::*;
#[test]
fn original_file_directory_receipts_match_only_the_real_retired_controller() {
    let root = Directory::new();
    let root = root.path().canonicalize().unwrap();
    // Hold the actual root directory until both native owners are finished.
    let payload: Vec<_> = (0..90000).map(|i| (i % 251) as u8).collect();
    std::fs::write(root.join("payload"), &payload).unwrap();
    let original = FileTreeNative::new(root.clone());
    original
        .execute(FileTreeOperation::Open(root.join("payload")))
        .unwrap();
    match original
        .execute(FileTreeOperation::Read {
            offset: 32767,
            length: 32768,
        })
        .unwrap()
    {
        FileTreeOutput::Bytes(bytes) => assert_eq!(bytes, &payload[32767..65535]),
        _ => panic!("real read"),
    }
    match original
        .execute(FileTreeOperation::List(root.clone()))
        .unwrap()
    {
        FileTreeOutput::Entries(entries) => assert!(entries.iter().any(|name| name == "payload")),
        _ => panic!("real directory"),
    }
    let other = FileTreeNative::new(root.clone());
    let receipt = original.close_originals().unwrap();
    assert!(receipt.matches(&original));
    assert!(!receipt.matches(&other));
    assert_eq!(receipt.closed_handles(), 2);
    assert!(
        original
            .execute(FileTreeOperation::Read {
                offset: 0,
                length: 1
            })
            .is_err(),
        "original is retired, not merely dropped"
    );
    assert_eq!(
        original.close_originals().unwrap().closed_handles(),
        0,
        "native receipts are consumed once"
    );
    other.close_originals().unwrap();
}
#[cfg(unix)]
#[test]
fn actual_root_escape_is_denied_and_fifo_is_closed_without_reading() {
    let root = Directory::new();
    let outside = Directory::new();
    std::fs::write(outside.path().join("private"), b"private").unwrap();
    std::os::unix::fs::symlink(outside.path(), root.path().join("escape")).unwrap();
    let root = root.path().canonicalize().unwrap();
    let original = FileTreeNative::new(root.clone());
    assert!(original
        .execute(FileTreeOperation::Open(root.join("escape/private")))
        .is_err());
    use std::os::unix::ffi::OsStrExt;
    let fifo = root.join("fifo");
    let path = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
    assert!(original.execute(FileTreeOperation::Open(fifo)).is_err());
    assert_eq!(
        original.close_originals().unwrap().closed_handles(),
        1,
        "same nonblocking FIFO original explicitly closed"
    );
}

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "zero-tree-original-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
