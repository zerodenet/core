//! Retain the real OS lock error as well as the existing path context.
use std::{
    io,
    path::{Path, PathBuf},
};
#[derive(Debug)]
struct LockAcquisition {
    state: PathBuf,
    lock: PathBuf,
    source: io::Error,
}
impl std::fmt::Display for LockAcquisition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "failed to acquire principal quota state lock `{}` for `{}`: {}",
            self.lock.display(),
            self.state.display(),
            self.source
        )
    }
}
impl std::error::Error for LockAcquisition {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
pub(super) fn retain(state: &Path, lock: &Path, source: io::Error) -> io::Error {
    io::Error::new(
        source.kind(),
        LockAcquisition {
            state: state.to_owned(),
            lock: lock.to_owned(),
            source,
        },
    )
}
