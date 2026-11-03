//! The cache operation retains its one actual file between bounded chunks.
use super::*;
use std::io::Write;
impl ConfigurationIo {
    pub(in crate::configuration) fn has_retained_failure(&self) -> bool {
        self.lock().failure.is_some()
    }
    pub(in crate::configuration) fn start_cache_file(
        &self,
        open: impl FnOnce() -> io::Result<File>,
    ) -> io::Result<()> {
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
        Ok(())
    }
    pub(in crate::configuration) fn write_cache_chunk(&self, bytes: &[u8]) -> io::Result<()> {
        let mut state = self.lock();
        if let Some(error) = &state.failure {
            return Err(retained(error));
        }
        state
            .original
            .as_mut()
            .ok_or_else(|| io::Error::other("cache original file is missing"))?
            .write_all(bytes)
    }
    /// Outer success is issued only after the original matches its real close
    /// receipt. The inner result separately preserves a flush/sync failure.
    pub(in crate::configuration) fn close_cache_file(
        &self,
        synchronize: bool,
    ) -> io::Result<io::Result<()>> {
        let mut state = self.lock();
        if let Some(error) = &state.failure {
            return Err(retained(error));
        }
        let original = state
            .original
            .as_mut()
            .ok_or_else(|| io::Error::other("cache original file is missing"))?;
        let operation = if synchronize {
            original.flush().and_then(|()| original.sync_all())
        } else {
            Ok(())
        };
        match original.close() {
            Ok(receipt) if original.matches_closed(&receipt) => {
                state.original = None;
                #[cfg(test)]
                {
                    state.closed += 1;
                }
                Ok(operation)
            }
            outcome => {
                let error = Arc::new(outcome.err().unwrap_or_else(|| {
                    io::Error::other("cache close receipt mismatched original")
                }));
                state.failure = Some(error.clone());
                Err(match operation {
                    Ok(()) => retained(&error),
                    Err(original) => OperationCloseFailure {
                        original,
                        recovery: retained(&error),
                    }
                    .into_io(),
                })
            }
        }
    }
}
