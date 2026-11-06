//! Poll the same caller cancellation without a second task or native owner.
use super::*;
use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
pub(super) struct CallerIo {
    io: BoxStream,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
    original: Arc<Transfer>,
}
impl CallerIo {
    pub(super) fn new(io: BoxStream, cancel: CancellationToken, original: Arc<Transfer>) -> Self {
        Self {
            io,
            cancelled: Box::pin(cancel.cancelled_owned()),
            original,
        }
    }
    fn check(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            self.original.stop.cancel();
            self.original.request_pending_retirement();
            self.original.adoption.request_retirement();
            return Err(io::ErrorKind::Interrupted.into());
        }
        Ok(())
    }
}
impl AsyncRead for CallerIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.check(cx)?;
        self.io.as_mut().poll_read(cx, bytes)
    }
}
impl AsyncWrite for CallerIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.check(cx)?;
        self.io.as_mut().poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check(cx)?;
        self.io.as_mut().poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check(cx)?;
        self.io.as_mut().poll_shutdown(cx)
    }
}

impl Drop for CallerIo {
    fn drop(&mut self) {
        self.original.stop.cancel();
        self.original.request_pending_retirement();
        self.original.adoption.request_retirement();
    }
}
