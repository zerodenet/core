//! Synchronous exact-path calls. The durable State owns every acquired file
//! before invoking domain parsing/encoding, including a panic at that boundary.
use super::*;
use slots::{failure, finish};
use std::{
    fs::OpenOptions,
    io::{Read, Write},
};
impl JournalGrant {
    /// Borrow this actual bounded reader for the complete parse. No descriptor
    /// or reader can escape in T. NotFound has no acquired read original.
    pub fn read_snapshot<T>(
        &self,
        read: impl FnOnce(&mut dyn Read) -> io::Result<T>,
    ) -> io::Result<Option<T>> {
        self.0.require_scope()?;
        let mut state = self.0.call_state()?;
        self.0.require_open(&state)?;
        state
            .read
            .close()
            .map_err(|source| failure(None, Some(source)))?;
        let file = match regular_options(&self.0.spec.path)?
            .read(true)
            .open(&self.0.spec.path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        state.read.install(file);
        let work = (|| {
            check_regular_size(state.read.original(), self.max_bytes())?;
            let mut reader = LimitedReader {
                file: state.read.original(),
                left: self.max_bytes(),
            };
            let result = read(&mut reader)?;
            check_regular_size(state.read.original(), self.max_bytes())?;
            Ok(Some(result))
        })();
        let closed = state.read.close();
        finish(work, closed)
    }
    /// Exact encoded length is measured before the borrowed encoder allocates.
    /// A partial append seals appends until a successful bounded replacement.
    pub fn append<T>(
        &self,
        encoded_bytes: usize,
        encode: impl FnOnce(&mut dyn Write) -> io::Result<T>,
    ) -> io::Result<T> {
        self.0.require_scope()?;
        let mut state = self.0.call_state()?;
        self.0.require_open(&state)?;
        if state.append_dirty {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "partial journal append requires replacement",
            ));
        }
        if state.append.confirmed() {
            let file = append_file(&self.0.spec.path)?;
            state.append.install(file);
            let valid = state.append.original().validate_regular();
            if let Err(error) = valid {
                let close = state.append.close();
                return Err(failure(Some(Arc::new(error)), close.err()));
            }
        }
        let current = check_regular_size(state.append.original(), self.max_bytes())?;
        let total = current.checked_add(encoded_bytes).ok_or_else(capacity)?;
        if total > self.max_bytes() {
            return Err(capacity());
        }
        state.append_dirty = true;
        let work = (|| {
            let mut writer = LimitedWriter {
                file: state.append.original(),
                left: encoded_bytes,
            };
            let value = encode(&mut writer)?;
            if writer.left != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "journal encoder length changed",
                ));
            }
            writer.flush()?;
            Ok(value)
        })();
        let value = work.map_err(|source| failure(Some(Arc::new(source)), None))?;
        state.append_dirty = false;
        Ok(value)
    }
    /// The complete next checkpoint must remain readable under this same cap.
    /// Existing append custody is closed before replace; the temporary file is
    /// strongly installed before encoding and positively closed before rename.
    pub fn replace<T>(
        &self,
        encoded_bytes: usize,
        encode: impl FnOnce(&mut dyn Write) -> io::Result<T>,
    ) -> io::Result<T> {
        if encoded_bytes > self.max_bytes() {
            return Err(capacity());
        }
        self.0.require_scope()?;
        let mut state = self.0.call_state()?;
        self.0.require_open(&state)?;
        state
            .append
            .close()
            .map_err(|source| failure(None, Some(source)))?;
        state
            .temporary
            .close()
            .map_err(|source| failure(None, Some(source)))?;
        state.discard_temporary()?;
        let parent = self
            .0
            .spec
            .path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| std::path::Path::new("."));
        let (file, mut path) = tempfile::NamedTempFile::new_in(parent)?.into_parts();
        path.disable_cleanup(true);
        state.temporary.install(file);
        state.temporary_path = Some(path);
        let work = (|| {
            let mut writer = LimitedWriter {
                file: state.temporary.original(),
                left: encoded_bytes,
            };
            let value = encode(&mut writer)?;
            if writer.left != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "journal checkpoint length changed",
                ));
            }
            writer.flush()?;
            state.temporary.original().sync_all()?;
            Ok(value)
        })();
        let closed = state.temporary.close();
        let value = finish(work, closed)?;
        let path = state
            .temporary_path
            .take()
            .expect("temporary installed before encoding");
        if let Err((path, error)) = super::rotation::persist(path, &self.0.spec.path) {
            state.temporary_path = Some(path);
            return Err(error);
        }
        state.append_dirty = false;
        Ok(value)
    }
    pub fn current_bytes(&self) -> io::Result<usize> {
        self.0.require_scope()?;
        let mut state = self.0.call_state()?;
        self.0.require_open(&state)?;
        if let Some(file) = state.append.file.as_ref() {
            if let Ok(metadata) = file.metadata() {
                return usize::try_from(metadata.len()).map_err(|_| capacity());
            }
        }
        // Read via the same retained position; no metadata-only unowned open.
        state
            .read
            .close()
            .map_err(|source| failure(None, Some(source)))?;
        let file = regular_options(&self.0.spec.path)?
            .read(true)
            .open(&self.0.spec.path)?;
        state.read.install(file);
        let work = check_regular_size(state.read.original(), self.max_bytes());
        let closed = state.read.close();
        finish(work, closed)
    }
}
pub(super) fn regular_options(path: &std::path::Path) -> io::Result<OpenOptions> {
    crate::file::validate_regular_path(path)?;
    #[cfg(unix)]
    let mut options = OpenOptions::new();
    #[cfg(not(unix))]
    let options = OpenOptions::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK);
    }
    Ok(options)
}
fn append_file(path: &std::path::Path) -> io::Result<std::fs::File> {
    let mut options = regular_options(path)?;
    options.read(true).append(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.share_mode(0x1 | 0x2 | 0x4);
    }
    options.open(path)
}
fn check_regular_size(file: &OriginalFile, limit: usize) -> io::Result<usize> {
    file.validate_regular()?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "journal original is not a regular file",
        ));
    }
    let size = usize::try_from(metadata.len()).map_err(|_| capacity())?;
    if size > limit {
        return Err(capacity());
    }
    Ok(size)
}
fn capacity() -> io::Error {
    io::Error::new(
        io::ErrorKind::FileTooLarge,
        "journal restart-readable byte envelope exceeded",
    )
}
struct LimitedReader<'a> {
    file: &'a mut OriginalFile,
    left: usize,
}
impl Read for LimitedReader<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        if self.left == 0 {
            return if self.file.read(&mut [0])? == 0 {
                Ok(0)
            } else {
                Err(capacity())
            };
        }
        let take = bytes.len().min(self.left);
        let count = self.file.read(&mut bytes[..take])?;
        self.left -= count;
        Ok(count)
    }
}
struct LimitedWriter<'a> {
    file: &'a mut OriginalFile,
    left: usize,
}
impl Write for LimitedWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.left {
            return Err(capacity());
        }
        let count = self.file.write(bytes)?;
        self.left -= count;
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}
