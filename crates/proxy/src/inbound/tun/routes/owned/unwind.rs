//! An original panic remains owned until native cleanup and its actual Scope exit.
use super::*;
use std::{
    any::Any,
    panic::{catch_unwind, AssertUnwindSafe},
};
pub(super) struct Panic {
    payload: Mutex<Option<Box<dyn Any + Send>>>,
}
impl Panic {
    pub(super) fn source(self: &Arc<Self>) -> SourceError {
        Arc::new(io::Error::other(self.clone()))
    }
    pub(super) fn resume(&self) -> ! {
        let payload = self
            .payload
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("same original worker panic");
        std::panic::resume_unwind(payload)
    }
}
impl std::fmt::Debug for Panic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OriginalRoutePanic")
    }
}
impl std::fmt::Display for Panic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original route responsibility unwound; actual panic retained")
    }
}
impl std::error::Error for Panic {}
pub(super) fn attempt<T>(work: impl FnOnce() -> T) -> Result<T, Arc<Panic>> {
    catch_unwind(AssertUnwindSafe(work)).map_err(|payload| {
        Arc::new(Panic {
            payload: Mutex::new(Some(payload)),
        })
    })
}
