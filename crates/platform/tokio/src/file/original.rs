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
