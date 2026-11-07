//! Private observation of one original IO's actual flush epochs.
//! Sender drain is checked by each protocol before requesting its epoch.
use futures_util::task::AtomicWaker;
use std::{
    error::Error,
    fmt, io,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
};
#[derive(Default)]
pub(crate) struct Flushed {
    requested: AtomicU64,
    completed: AtomicU64,
    ended: AtomicBool,
    failure: Mutex<Option<Arc<io::Error>>>,
    driver: AtomicWaker,
    writer: AtomicWaker,
    changed: tokio::sync::Notify,
}
#[derive(Debug)]
struct Original(Arc<io::Error>);
impl fmt::Display for Original {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl Error for Original {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}
fn retained(error: &Arc<io::Error>) -> io::Error {
    io::Error::new(error.kind(), Original(error.clone()))
}
impl Flushed {
    pub(crate) fn request(&self) -> io::Result<u64> {
        self.check()?;
        let old = self
            .requested
            .try_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                old.checked_add(1)
            })
            .map_err(|_| io::Error::other("original native flush epoch exhausted"))?;
        self.driver.wake();
        Ok(old + 1)
    }
    pub(crate) fn check(&self) -> io::Result<()> {
        let failure = self.failure.lock().unwrap().clone();
        match failure {
            Some(failure) => Err(retained(&failure)),
            None => Ok(()),
        }
    }
    fn ready(&self, target: u64) -> io::Result<bool> {
        self.check()?;
        if self.completed.load(Ordering::Acquire) >= target {
            return Ok(true);
        }
        if self.ended.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "original connection ended before requested native flush",
            ));
        }
        Ok(false)
    }
    #[cfg(feature = "h2")]
    pub(crate) fn poll(&self, cx: &mut Context<'_>, target: u64) -> Poll<io::Result<()>> {
        self.writer.register(cx.waker());
        if self.ready(target)? {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    #[cfg(feature = "grpc")]
    pub(crate) async fn wait(&self, target: u64) -> io::Result<()> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.ready(target)? {
                return Ok(());
            }
            changed.await;
        }
    }
    pub(crate) fn fail(&self, error: io::Error) -> io::Error {
        let incoming = Arc::new(error);
        let saved = {
            self.failure
                .lock()
                .unwrap()
                .get_or_insert_with(|| incoming.clone())
                .clone()
        };
        drop(incoming);
        self.notify();
        retained(&saved)
    }
    pub(crate) fn finish(&self, result: io::Result<()>) -> io::Result<()> {
        let result = result.map_err(|error| self.fail(error));
        // Future completion is only terminal metadata, never a flush epoch.
        self.ended.store(true, Ordering::Release);
        self.notify();
        result
    }
    pub(crate) fn interrupt(&self) {
        // Drop/cancel is only a wake/error condition, never flush/closure proof.
        self.ended.store(true, Ordering::Release);
        self.notify();
    }
    fn notify(&self) {
        self.writer.wake();
        self.changed.notify_waiters();
    }
}
/// Per-IO in-flight state: an old Pending flush cannot acknowledge a request
/// made after it started, including frames buffered while that flush was Pending.
pub(crate) struct NativeFlush {
    state: Arc<Flushed>,
    started: Option<u64>,
}
impl NativeFlush {
    pub(crate) fn new(state: Arc<Flushed>) -> Self {
        Self {
            state,
            started: None,
        }
    }
    pub(crate) fn bind(&self, cx: &Context<'_>) {
        self.state.driver.register(cx.waker());
    }
    pub(crate) fn poll<S: tokio::io::AsyncWrite + Unpin>(
        &mut self,
        io: &mut S,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        self.state.driver.register(cx.waker());
        let epoch = *self
            .started
            .get_or_insert_with(|| self.state.requested.load(Ordering::Acquire));
        match std::pin::Pin::new(io).poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                self.started = None;
                let prior = self.state.completed.fetch_max(epoch, Ordering::AcqRel);
                if epoch > prior {
                    self.state.notify();
                }
                if self.state.requested.load(Ordering::Acquire) > epoch {
                    // A request after the old flush started still needs its
                    // actual next flush. Keep the same codec continuation
                    // Pending so normal connection progress cannot discard it.
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Poll::Ready(Ok(()))
                }
            }
            Poll::Ready(Err(error)) => {
                self.started = None;
                Poll::Ready(Err(self.state.fail(error)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
/// Kept by the existing connection future. It does not consume any task/join.
#[cfg(feature = "grpc")]
pub(crate) struct DriverGuard(pub(crate) Arc<Flushed>);
#[cfg(feature = "grpc")]
impl Drop for DriverGuard {
    fn drop(&mut self) {
        self.0.interrupt();
    }
}

#[cfg(all(test, any(unix, windows)))]
pub(crate) mod test_fixture;
#[cfg(all(test, any(unix, windows)))]
mod tests;
