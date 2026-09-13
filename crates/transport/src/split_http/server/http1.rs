//! Preserve packet uploads already received when an upload peer stops reading.
//!
//! The pinned reference's raw HTTP/1 pool does not wait for packet replies and
//! Go GC can close a pooled connection with more requests already in flight.
//! A failed acknowledgement must not discard the remaining received requests.
//! Stream/download replies still fail immediately and cancel their owner.
use std::{
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Clone, Default)]
pub(super) struct ReplyPolicy(Arc<AtomicBool>);
impl ReplyPolicy {
    pub(super) fn set_packet(&self, packet: bool) {
        self.0.store(packet, Ordering::Release);
    }
    fn packet(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}
pub(super) struct UploadDrain<S> {
    inner: S,
    policy: ReplyPolicy,
    write_failed: bool,
}
impl<S> UploadDrain<S> {
    pub(super) fn new(inner: S, policy: ReplyPolicy) -> Self {
        Self {
            inner,
            policy,
            write_failed: false,
        }
    }
    fn discard_failed_reply(&mut self, error: &io::Error) -> bool {
        if self.policy.packet()
            && matches!(
                error.kind(),
                io::ErrorKind::BrokenPipe
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionAborted
            )
        {
            self.write_failed = true;
            tracing::trace!(%error, "XHTTP packet reply peer closed; draining received uploads");
            true
        } else {
            false
        }
    }
    fn failed(&self) -> io::Error {
        io::Error::new(io::ErrorKind::BrokenPipe, "HTTP response peer closed")
    }
}
impl<S: AsyncRead + Unpin> AsyncRead for UploadDrain<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for UploadDrain<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.write_failed {
            return Poll::Ready(if self.policy.packet() {
                Ok(bytes.len())
            } else {
                Err(self.failed())
            });
        }
        match Pin::new(&mut self.inner).poll_write(cx, bytes) {
            Poll::Ready(Err(error)) if self.discard_failed_reply(&error) => {
                Poll::Ready(Ok(bytes.len()))
            }
            result => result,
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.write_failed {
            return Poll::Ready(if self.policy.packet() {
                Ok(())
            } else {
                Err(self.failed())
            });
        }
        match Pin::new(&mut self.inner).poll_flush(cx) {
            Poll::Ready(Err(error)) if self.discard_failed_reply(&error) => Poll::Ready(Ok(())),
            result => result,
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
#[cfg(test)]
#[path = "../../../tests/xhttp_server/upload_close.rs"]
mod tests;
