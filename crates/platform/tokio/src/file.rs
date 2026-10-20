//! Blocking regular-file reads. The caller must retain and join the real job;
//! cancelling its waiter does not interrupt a filesystem operation.
use std::{io, path::Path};

/// Read the exact supplied path without canonicalizing its authority. Unix
/// follows the usual bounded kernel symlink lookup, opens nonblocking to avoid
/// waiting for a FIFO peer, then validates the actual opened descriptor. Regular
/// filesystem I/O itself may block; this function never spawns a detached job.
/// Other platforms explicitly return Unsupported until their native open policy
/// is implemented and qualified; Unix results do not certify Windows file access.
pub fn read_regular_file(path: &Path, max_bytes: usize) -> io::Result<Vec<u8>> {
    let limit = max_bytes
        .checked_add(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "file byte bound overflow"))?;
    #[cfg(unix)]
    {
        use std::{fs::OpenOptions, io::Read, os::unix::fs::OpenOptionsExt};
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(path)?;
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
        file.take(limit as u64).read_to_end(&mut bytes)?;
        if bytes.len() > max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "declared file grew past its byte bound",
            ));
        }
        Ok(bytes)
    }
    #[cfg(not(unix))]
    {
        let _ = (path, limit);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "regular-file grants require a platform-specific nonblocking open policy",
        ))
    }
}

#[cfg(all(test, unix))]
#[path = "../tests/file.rs"]
mod tests;
