//! The same actual task returns its original panic and competing cleanup faults.
use std::{io, sync::Mutex};
pub(crate) fn finish_caught(
    work: Result<io::Result<()>, Box<dyn std::any::Any + Send>>,
    cleanup: impl IntoIterator<Item = io::Result<()>>,
) -> io::Result<()> {
    match work {
        Ok(work) => super::finish(std::iter::once(work).chain(cleanup)),
        Err(payload) => {
            let cleanup = cleanup
                .into_iter()
                .filter_map(Result::err)
                .collect::<Vec<_>>();
            if cleanup.is_empty() {
                std::panic::resume_unwind(payload);
            }
            Err(io::Error::other(PanicCleanup {
                payload: Mutex::new(payload),
                cleanup,
            }))
        }
    }
}
struct PanicCleanup {
    payload: Mutex<Box<dyn std::any::Any + Send>>,
    cleanup: Vec<io::Error>,
}
impl std::fmt::Debug for PanicCleanup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for PanicCleanup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // No foreign formatter is called under the original Box payload lock.
        let payload = self
            .payload
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(message) = payload.downcast_ref::<String>() {
            write!(f, "original gRPC work panicked: {message}")?;
        } else if let Some(message) = payload.downcast_ref::<&str>() {
            write!(f, "original gRPC work panicked: {message}")?;
        } else {
            f.write_str("original gRPC work panicked (actual payload retained)")?;
        }
        drop(payload);
        write!(
            f,
            "; {} actual cleanup failures retained",
            self.cleanup.len()
        )
    }
}
impl std::error::Error for PanicCleanup {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cleanup.first().map(|source| source as _)
    }
}
#[cfg(test)]
mod tests;
