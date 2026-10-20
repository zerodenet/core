use super::users::Permit;
use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::sync::WaitForCancellationFutureOwned;

/// Same actual stream first, then its permit: dropping this sealed wrapper
/// releases the raw stream before returning user capacity. It proves no FD close.
pub struct OwnedSendStream {
    stream: quinn::SendStream,
    cancelled: Pin<Box<WaitForCancellationFutureOwned>>,
    permit: Permit,
}
pub struct OwnedRecvStream {
    stream: quinn::RecvStream,
    cancelled: Pin<Box<WaitForCancellationFutureOwned>>,
    permit: Permit,
}
impl OwnedSendStream {
    pub(super) fn new(stream: quinn::SendStream, permit: Permit) -> Self {
        let cancelled = Box::pin(permit.users().cancellation().clone().cancelled_owned());
        Self {
            stream,
            permit,
            cancelled,
        }
    }
    pub fn finish(&mut self) -> Result<(), quinn::ClosedStream> {
        self.stream.finish()
    }
    pub fn reset(&mut self, code: quinn::VarInt) -> Result<(), quinn::ClosedStream> {
        self.stream.reset(code)
    }
    pub fn id(&self) -> quinn::StreamId {
        self.stream.id()
    }
    pub fn user_count(&self) -> usize {
        self.permit.users().active()
    }
}
impl OwnedRecvStream {
    pub(super) fn new(stream: quinn::RecvStream, permit: Permit) -> Self {
        let cancelled = Box::pin(permit.users().cancellation().clone().cancelled_owned());
        Self {
            stream,
            permit,
            cancelled,
        }
    }
    pub fn stop(&mut self, code: quinn::VarInt) -> Result<(), quinn::ClosedStream> {
        self.stream.stop(code)
    }
    pub fn id(&self) -> quinn::StreamId {
        self.stream.id()
    }
    pub fn user_count(&self) -> usize {
        self.permit.users().active()
    }
}
fn cancelled(future: &mut Pin<Box<WaitForCancellationFutureOwned>>, cx: &mut Context<'_>) -> bool {
    future.as_mut().poll(cx).is_ready()
}
fn stopped() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "owned QUIC user was cancelled")
}
impl AsyncRead for OwnedRecvStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if cancelled(&mut this.cancelled, cx) {
            return Poll::Ready(Err(stopped()));
        }
        AsyncRead::poll_read(Pin::new(&mut this.stream), cx, bytes)
    }
}
impl AsyncWrite for OwnedSendStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if cancelled(&mut this.cancelled, cx) {
            return Poll::Ready(Err(stopped()));
        }
        AsyncWrite::poll_write(Pin::new(&mut this.stream), cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if cancelled(&mut this.cancelled, cx) {
            return Poll::Ready(Err(stopped()));
        }
        AsyncWrite::poll_flush(Pin::new(&mut this.stream), cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if cancelled(&mut this.cancelled, cx) {
            return Poll::Ready(Err(stopped()));
        }
        AsyncWrite::poll_shutdown(Pin::new(&mut this.stream), cx)
    }
}
