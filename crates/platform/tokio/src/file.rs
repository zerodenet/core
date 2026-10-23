//! Blocking regular-file reads. The caller must retain and join the real job;
//! cancelling its waiter does not interrupt a filesystem operation.
use std::{fs::File, io, path::Path};

#[cfg(windows)]
mod windows;

/// Read the exact supplied path without canonicalizing its authority. Unix
/// follows the usual bounded kernel symlink lookup, opens nonblocking to avoid
/// waiting for a FIFO peer, then validates the actual opened descriptor. Regular
/// filesystem I/O itself may block; this function never spawns a detached job.
/// Windows rejects explicit device/pipe namespaces before opening, then validates
/// the actual disk handle before reading. Ordinary symlinks follow kernel lookup;
/// the supplied authority is never rewritten. File I/O may block on either OS.
pub fn read_regular_file(path: &Path, max_bytes: usize) -> io::Result<Vec<u8>> {
    let owner = PendingFileRead::new(path.to_path_buf(), max_bytes)?;
    let result = owner.run();
    drop(owner);
    result.map_err(|source| {
        Arc::try_unwrap(source)
            .unwrap_or_else(|source| io::Error::new(source.kind(), RetainedIo(source)))
    })
}

mod native;
mod owner;
pub use owner::{FileNativeEvidence, FileReadEvidence, PendingFileRead};
use std::sync::Arc;
#[derive(Debug)]
struct RetainedIo(Arc<io::Error>);
impl std::fmt::Display for RetainedIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for RetainedIo {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

fn read_opened_file(file: &mut File, max_bytes: usize, limit: usize) -> io::Result<Vec<u8>> {
    use std::io::Read;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "declared resource is not an opened regular file",
        ));
    }
    if metadata.len() > max_bytes as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "declared file byte bound exceeded",
        ));
    }
    let mut bytes = Vec::new();
    (&mut *file).take(limit as u64).read_to_end(&mut bytes)?;
    if bytes.len() > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "declared file grew past its byte bound",
        ));
    }
    Ok(bytes)
}

#[cfg(all(test, unix))]
#[path = "../tests/file/unix.rs"]
mod tests;

#[cfg(all(test, windows))]
#[path = "../tests/file/windows.rs"]
mod windows_tests;
