//! One synchronous original-file slot belonging to the actual configuration owner.
use std::{
    fmt,
    fs::File,
    io::{self, Read},
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
};
use zero_platform_tokio::file::OriginalFile;
mod cache;

#[derive(Default, Debug)]
pub(super) struct ConfigurationIo {
    state: Mutex<IoState>,
}
#[derive(Default, Debug)]
struct IoState {
    original: Option<OriginalFile>,
    failure: Option<Arc<io::Error>>,
    #[cfg(test)]
    acquired: usize,
    #[cfg(test)]
    closed: usize,
}
#[derive(Debug)]
struct RetainedIo(Arc<io::Error>);
impl fmt::Display for RetainedIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for RetainedIo {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
fn retained(error: &Arc<io::Error>) -> io::Error {
    io::Error::new(error.kind(), RetainedIo(error.clone()))
}
impl ConfigurationIo {
    #[cfg(test)]
    pub(super) fn original_observation(&self) -> (usize, usize, bool, bool) {
        let state = self.lock();
        (
            state.acquired,
            state.closed,
            state.original.is_some(),
            state.failure.is_some(),
        )
    }

    fn lock(&self) -> MutexGuard<'_, IoState> {
        self.state.lock().unwrap_or_else(|error| {
            let mut state = error.into_inner();
            state.failure.get_or_insert_with(|| {
                Arc::new(io::Error::other(
                    "original configuration file operation panicked; cleanup unconfirmed",
                ))
            });
            state
        })
    }
    pub(super) fn ensure_available(&self) -> io::Result<()> {
        let state = self.lock();
        if let Some(error) = &state.failure {
            return Err(retained(error));
        }
        if state.original.is_some() {
            return Err(io::Error::other(
                "configuration original file has not closed",
            ));
        }
        Ok(())
    }
    pub(super) fn with_file<T>(
        &self,
        open: impl FnOnce() -> io::Result<File>,
        operation: impl FnOnce(&mut OriginalFile) -> io::Result<T>,
    ) -> io::Result<T> {
        let mut state = self.lock();
        if let Some(error) = &state.failure {
            return Err(retained(error));
        }
        if state.original.is_some() {
            return Err(io::Error::other(
                "configuration original file slot occupied",
            ));
        }
        state.original = Some(OriginalFile::adopt(open()?));
        #[cfg(test)]
        {
            state.acquired += 1;
        }
        let result = operation(state.original.as_mut().unwrap());
        let original = state.original.as_mut().unwrap();
        let closed = original.close();
        match closed {
            Ok(receipt) if original.matches_closed(&receipt) => {
                state.original = None;
                #[cfg(test)]
                {
                    state.closed += 1;
                }
                result
            }
            outcome => {
                let error = outcome.err().unwrap_or_else(|| {
                    io::Error::other("configuration file close receipt mismatched its original")
                });
                let error = Arc::new(error);
                state.failure = Some(error.clone());
                match result {
                    Err(original) => Err(OperationCloseFailure {
                        original,
                        recovery: retained(&error),
                    }
                    .into_io()),
                    Ok(_) => Err(retained(&error)),
                }
            }
        }
    }
    pub(super) fn read_text(&self, path: &Path) -> io::Result<String> {
        self.with_file(
            || File::open(path),
            |file| {
                let mut text = String::new();
                file.read_to_string(&mut text)?;
                Ok(text)
            },
        )
    }
    pub(super) fn read_limited(&self, path: &Path, max_bytes: usize) -> io::Result<Vec<u8>> {
        let limit = max_bytes
            .checked_add(1)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "configuration material limit overflow",
                )
            })?;
        self.with_file(
            || File::open(path),
            |file| {
                let mut bytes = Vec::new();
                file.take(limit).read_to_end(&mut bytes)?;
                if bytes.len() > max_bytes {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "configuration material exceeds its declared byte limit",
                    ));
                }
                Ok(bytes)
            },
        )
    }

    pub(super) fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        self.with_file(
            || File::open(path),
            |file| {
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes)?;
                Ok(bytes)
            },
        )
    }
}

#[derive(Debug)]
struct OperationCloseFailure {
    original: io::Error,
    recovery: io::Error,
}
impl fmt::Display for OperationCloseFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "configuration file operation failed: {}; original close also failed: {}",
            self.original, self.recovery
        )
    }
}
impl std::error::Error for OperationCloseFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.original)
    }
}
impl OperationCloseFailure {
    fn into_io(self) -> io::Error {
        io::Error::new(self.original.kind(), self)
    }
}
