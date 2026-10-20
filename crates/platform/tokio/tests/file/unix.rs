use super::*;
use std::{
    ffi::CString,
    fs,
    os::unix::fs::symlink,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "zero-file-grant-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
#[test]
fn actual_regular_file_is_bounded_and_symlink_reads_use_the_opened_file() {
    let directory = Directory::new();
    let actual = directory.0.join("actual");
    fs::write(&actual, b"four").unwrap();
    let link = directory.0.join("link");
    symlink(&actual, &link).unwrap();
    assert_eq!(read_regular_file(&link, 4).unwrap(), b"four");
    assert_eq!(
        read_regular_file(&actual, 3).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        read_regular_file(&actual, usize::MAX).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        read_regular_file(&directory.0.join("absent"), 4)
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
}
#[test]
fn an_actual_fifo_never_waits_for_a_peer_and_opened_nonregular_files_are_rejected() {
    use std::os::unix::ffi::OsStrExt;
    let directory = Directory::new();
    let fifo = directory.0.join("pipe");
    let encoded = CString::new(fifo.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(encoded.as_ptr(), 0o600) }, 0);
    assert_eq!(
        read_regular_file(&fifo, 64).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        read_regular_file(&directory.0, 64).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}
