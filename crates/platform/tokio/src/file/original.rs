//! Sole original regular-file ownership for retained application resources.
use super::{native, RetainedIo};
use std::{
    fs::{File, Metadata, TryLockError},
    io::{self, Read, Seek, SeekFrom, Write},
    sync::Arc,
};

/// An actual successful native close, bound to this exact original owner.
#[derive(Clone, Debug)]
pub struct OriginalFileClosed(Arc<()>);
/// Adopt immediately after successful open and retain outside cancellable work.
/// No descriptor duplication or consuming conversion back to File is exposed.
/// Drop provides emergency disposal only; it never produces a close receipt.
pub struct OriginalFile {
    file: Option<File>,
    identity: Arc<()>,
    outcome: Option<Result<OriginalFileClosed, Arc<io::Error>>>,
}
impl std::fmt::Debug for OriginalFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginalFile")
            .field("close_observed", &self.outcome.is_some())
            .finish_non_exhaustive()
    }
}
impl OriginalFile {
    /// Pure peak liability for this provider's identity, sentinel/actual-error
    /// Arc headers and its retained error wrapper. No file/close authority.
    /// This excludes opaque std::io formatting/OS bookkeeping and process RSS.
    /// The actual caller must keep its promise through any retained source.
    pub fn retained_header_layout() -> Result<
        zero_traits::packet_metadata::PacketMetadataLayout,
        zero_traits::packet_metadata::PacketMemoryError,
    > {
        use zero_traits::packet_metadata::PacketMetadataLayout as L;
        L::arc::<()>()?
            .checked_add(L::arc::<io::Error>()?)?
            .checked_add(L::arc::<io::Error>()?)?
            .checked_add(L::new(1, std::mem::size_of::<RetainedIo>()))
    }
    pub fn adopt(file: File) -> Self {
        Self {
            file: Some(file),
            identity: Arc::new(()),
            outcome: None,
        }
    }
    fn original(&self) -> io::Result<&File> {
        self.file
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "original file retired"))
    }
    fn original_mut(&mut self) -> io::Result<&mut File> {
        self.file
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "original file retired"))
    }
    /// Call after all actual workers/users have returned the unique owner.
    /// On failure, the same unknown disposition and source remain retained;
    /// retrying a possibly reused descriptor is forbidden.
    pub fn close(&mut self) -> io::Result<OriginalFileClosed> {
        if let Some(outcome) = &self.outcome {
            return outcome
                .clone()
                .map_err(|source| io::Error::new(source.kind(), RetainedIo(source)));
        }
        let sentinel = Arc::new(io::Error::other(
            "original file close disposition not observed",
        ));
        self.outcome = Some(Err(sentinel));
        let file = self
            .file
            .take()
            .expect("original file installed until close");
        let outcome = native::close(file)
            .map(|()| OriginalFileClosed(self.identity.clone()))
            .map_err(Arc::new);
        self.outcome = Some(outcome.clone());
        outcome.map_err(|source| io::Error::new(source.kind(), RetainedIo(source)))
    }
    pub fn matches_closed(&self, receipt: &OriginalFileClosed) -> bool {
        Arc::ptr_eq(&self.identity, &receipt.0) && self.outcome.as_ref().is_some_and(Result::is_ok)
    }
    /// Map the same retained original without cloning or exporting its handle.
    /// The returned mapping is immutable snapshot data; a file-close receipt
    /// proves descriptor closure only, not unmapping of that domain value.
    ///
    /// # Safety
    /// The caller must ensure the backing file is not modified or truncated
    /// for the mapping's entire lifetime, as required by `memmap2`.
    pub unsafe fn map_readonly(&self) -> io::Result<memmap2::Mmap> {
        // SAFETY: the caller accepts the backing-file lifetime obligation.
        unsafe { memmap2::MmapOptions::new().map(self.original()?) }
    }
    /// Internal Linux directory enumeration on this same retained original.
    /// No RawFd, duplicate or consuming File escapes its matching close owner.
    #[cfg(target_os = "linux")]
    pub(crate) fn read_linux_directory_entries(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        use std::os::fd::AsRawFd;
        let result = unsafe {
            libc::syscall(
                libc::SYS_getdents64,
                self.original()?.as_raw_fd(),
                bytes.as_mut_ptr(),
                bytes.len(),
            )
        };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(result as usize)
        }
    }

    /// Check this actual original before a journal performs any IO.
    pub(crate) fn validate_regular(&self) -> io::Result<()> {
        #[cfg(windows)]
        super::windows::validate(self.original()?)?;
        if !self.metadata()?.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "journal original is not a regular file",
            ));
        }
        Ok(())
    }
    pub fn metadata(&self) -> io::Result<Metadata> {
        self.original()?.metadata()
    }
    pub fn sync_all(&self) -> io::Result<()> {
        self.original()?.sync_all()
    }
    pub fn sync_data(&self) -> io::Result<()> {
        self.original()?.sync_data()
    }
    pub fn set_len(&self, size: u64) -> io::Result<()> {
        self.original()?.set_len(size)
    }
    pub fn try_lock(&self) -> Result<(), TryLockError> {
        self.original().map_err(TryLockError::Error)?.try_lock()
    }
    pub fn unlock(&self) -> io::Result<()> {
        self.original()?.unlock()
    }
}
impl Read for OriginalFile {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.original_mut()?.read(bytes)
    }
}
impl Write for OriginalFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.original_mut()?.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.original_mut()?.flush()
    }
}
impl Seek for OriginalFile {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.original_mut()?.seek(position)
    }
}

#[cfg(test)]
#[path = "../../tests/file/original.rs"]
mod tests;
