use super::*;
use std::{
    io,
    pin::Pin,
    sync::atomic::Ordering,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub struct LoanIo {
    io: Option<BoxStream>,
    state: Arc<State>,
    cancelled: Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
}
impl LoanIo {
    /// Retain a host's existing custody/revocation wrapper around the same view.
    /// This neither changes its donor origin nor creates close authority.
    pub fn wrap(&mut self, wrap: impl FnOnce(BoxStream) -> BoxStream) {
        self.io = Some(wrap(self.io.take().expect("live loan view")));
    }
    pub(super) fn new(io: BoxStream, state: Arc<State>) -> Self {
        let cancelled = Box::pin(state.cancel.clone().cancelled_owned());
        Self {
            io: Some(io),
            state,
            cancelled,
        }
    }
    fn check(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            Err(io::ErrorKind::BrokenPipe.into())
        } else {
            Ok(())
        }
    }
}
impl AsyncRead for LoanIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.check(cx)?;
        self.io
            .as_mut()
            .expect("live loan view")
            .as_mut()
            .poll_read(cx, bytes)
    }
}
impl AsyncWrite for LoanIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.check(cx)?;
        self.io
            .as_mut()
            .expect("live loan view")
            .as_mut()
            .poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check(cx)?;
        self.io
            .as_mut()
            .expect("live loan view")
            .as_mut()
            .poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check(cx)?;
        self.io
            .as_mut()
            .expect("live loan view")
            .as_mut()
            .poll_shutdown(cx)
    }
}
impl Drop for LoanIo {
    fn drop(&mut self) {
        // Dispose the actual view before publishing its return, including any
        // reentrant I/O destructors. This is custody only, never native close.
        let previous = self.io.take();
        drop(previous);
        self.state.returned.store(true, Ordering::Release);
        self.state.changed.notify_waiters();
    }
}
