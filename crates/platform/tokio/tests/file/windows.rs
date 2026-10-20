//! Native Windows regression cases. Cross-compilation is not execution evidence.
use super::*;
use std::{
    fs,
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
fn actual_disk_file_is_bounded_and_native_open_errors_are_preserved() {
    let directory = Directory::new();
    let path = directory.0.join("actual");
    fs::write(&path, b"four").unwrap();
    assert_eq!(read_regular_file(&path, 4).unwrap(), b"four");
    assert_eq!(
        read_regular_file(&path, 3).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        read_regular_file(&path, usize::MAX).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    let missing = read_regular_file(&directory.0.join("absent"), 4).unwrap_err();
    assert_eq!(missing.kind(), io::ErrorKind::NotFound);
    assert_eq!(missing.raw_os_error(), Some(2)); // ERROR_FILE_NOT_FOUND
    assert!(read_regular_file(&directory.0, 4).is_err());
}

#[test]
fn actual_character_device_and_explicit_device_namespaces_are_rejected() {
    assert_eq!(
        read_regular_file(Path::new("NUL"), 64).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    for path in [
        r"\\.\PhysicalDrive0",
        r"\\.\C:",
        r"\\?\C:",
        r"\\?\GLOBALROOT\Device\HarddiskVolume1\file",
        r"\\localhost\pipe\zero-file-grant",
        r"\\?\UNC\localhost\pipe\zero-file-grant",
    ] {
        assert_eq!(
            read_regular_file(Path::new(path), 64).unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
    }
}

#[tokio::test]
async fn an_actual_silent_pipe_handle_is_rejected_before_any_read() {
    use std::os::windows::io::AsRawHandle;
    use tokio::net::windows::named_pipe::ServerOptions;
    use windows_sys::Win32::{
        Foundation::ERROR_PIPE_BUSY,
        Storage::FileSystem::{GetFileType, FILE_TYPE_PIPE},
    };
    let directory = Directory::new();
    let name = format!(
        r"\\.\pipe\zero-file-grant-{}-{}",
        std::process::id(),
        directory.0.file_name().unwrap().to_string_lossy()
    );
    let _server = ServerOptions::new()
        .access_inbound(false)
        .access_outbound(true)
        .max_instances(1)
        .create(&name)
        .unwrap();
    // No writer or detached task will produce data. Opening is a single native
    // attempt; pipe busy is a native error, not a request to wait for a server.
    let client = File::open(&name).unwrap();
    assert_eq!(
        unsafe { GetFileType(client.as_raw_handle()) },
        FILE_TYPE_PIPE
    );
    assert_eq!(
        File::open(&name).unwrap_err().raw_os_error(),
        Some(ERROR_PIPE_BUSY as i32)
    );
    assert_eq!(
        windows::test_read_opened(client).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        read_regular_file(Path::new(&name), 64).unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
}
