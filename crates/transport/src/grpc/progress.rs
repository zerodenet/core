//! Preserve the actual writer error while sharing the existing byte/wakeup rules.
use std::{
    error::Error,
    fmt, io,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
#[derive(Default)]
pub(super) struct Progress {
    inner: crate::stream::progress::Progress,
    failure: Mutex<Option<Arc<io::Error>>>,
}
#[derive(Debug)]
struct OriginalFailure(Arc<io::Error>);
impl fmt::Display for OriginalFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl Error for OriginalFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}
impl Progress {
    pub(super) fn accept(&self, count: usize) {
        self.inner.accept(count);
    }
    pub(super) fn commit(&self, count: usize) {
        self.inner.commit(count);
    }
    pub(super) fn finish(&self) {
        self.inner.finish();
    }
    pub(super) fn fail(&self, error: io::Error) {
        let mut failure = self.failure.lock().unwrap();
        let saved = failure.get_or_insert_with(|| Arc::new(error)).clone();
        drop(failure);
        self.inner.fail(saved.as_ref());
    }
    pub(super) fn check(&self) -> io::Result<()> {
        match self.failure.lock().unwrap().as_ref() {
            Some(error) => Err(io::Error::new(error.kind(), OriginalFailure(error.clone()))),
            None => Ok(()),
        }
    }
    pub(super) fn poll(&self, cx: &mut Context<'_>, shutdown: bool) -> Poll<io::Result<()>> {
        self.check()?;
        // Failure is stored before the underlying progress waker is notified.
        // If it races this first check, recover that same retained object.
        self.inner
            .poll(cx, shutdown)
            .map_err(|fallback| self.check().err().unwrap_or(fallback))
    }
}
