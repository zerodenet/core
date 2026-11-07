//! Borrow original source-write facts without cloning an arbitrary error text.
//! No file open, recovery, write, registration or close permission is issued.
use super::*;
use std::sync::MutexGuard;
pub struct SourceWriteView<'a> {
    state: MutexGuard<'a, SourceState>,
    unknown: bool,
}
impl ConfigurationSource {
    /// Source is outermost in the configuration publication lock order. The
    /// caller must take this before Engine/traffic/endpoint membership guards,
    /// and release every view before awaiting or returning a byte permission.
    pub fn read_write_observation(&self) -> SourceWriteView<'_> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let unknown = self.ensure_resource_io_available().is_err();
        SourceWriteView { state, unknown }
    }
}
impl SourceWriteView<'_> {
    pub fn writable(&self) -> Option<bool> {
        if self.unknown {
            None
        } else {
            self.state.observation.writable
        }
    }
    pub fn observed_at_unix_ms(&self) -> Option<u64> {
        self.state.observation.observed_at_unix_ms
    }
    pub fn reason(&self) -> Option<&str> {
        if self.unknown {
            Some("source_file_cleanup_unconfirmed")
        } else {
            self.state.observation.reason.as_deref()
        }
    }
}
impl std::fmt::Debug for SourceWriteView<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceWriteView").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
