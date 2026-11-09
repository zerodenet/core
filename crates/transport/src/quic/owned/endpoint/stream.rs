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
type FinishFuture = Pin<Box<dyn Future<Output = io::Result<()>> + Send + Sync>>;
pub struct OwnedSendStream {
    stream: quinn::SendStream,
    finish: Option<FinishFuture>,
    finished: bool,
    cancelled: Pin<Box<WaitForCancellationFutureOwned>>,
    permit: Permit,
}
pub struct OwnedRecvStream {
    stream: quinn::RecvStream,
    cancelled: Pin<Box<WaitForCancellationFutureOwned>>,
    permit: Permit,
}
impl OwnedSendStream {
    #[cfg(any(feature = "split_http", feature = "hysteria", feature = "quic_h3"))]
    pub(super) fn into_h3(self) -> super::h3::SendStream {
        let Self {
            stream,
            cancelled,
            permit,
            finish: _,
            finished: _,
        } = self;
        super::h3::SendStream::new(stream, cancelled, permit)
    }

    pub(super) fn new(stream: quinn::SendStream, permit: Permit) -> Self {
        let cancelled = Box::pin(permit.users().cancellation().clone().cancelled_owned());
        Self {
            stream,
            permit,
            cancelled,
            finish: None,
            finished: false,
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
    #[cfg(any(feature = "split_http", feature = "hysteria", feature = "quic_h3"))]
    pub(super) fn into_h3(self) -> super::h3::RecvStream {
        let Self {
            stream,
            cancelled,
            permit,
        } = self;
        super::h3::RecvStream::new(stream, cancelled, permit)
    }

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
    // Cancellation is terminal; read helpers retry Interrupted indefinitely.
    io::Error::new(io::ErrorKind::BrokenPipe, "owned QUIC user was cancelled")
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
        if this.finished {
            return Poll::Ready(Ok(()));
        }
        if this.finish.is_none() {
            std::task::ready!(AsyncWrite::poll_shutdown(Pin::new(&mut this.stream), cx))?;
            // Quinn's poll_shutdown only queues FIN. Keep the same original
            // stopped future in THIS stream, across cancellation of a borrowing
            // shutdown waiter, until all accepted data+FIN are really ACKed.
            let stopped = this.stream.stopped();
            this.finish = Some(Box::pin(async move {
                match stopped.await.map_err(io::Error::other)? {
                    None => Ok(()),
                    Some(code) => Err(io::Error::new(io::ErrorKind::BrokenPipe, PeerStopped(code))),
                }
            }));
        }
        let result = std::task::ready!(this.finish.as_mut().unwrap().as_mut().poll(cx));
        this.finished = result.is_ok();
        // Dispose the actual original future before publishing its terminal
        // result. This is a write-half ACK, never native/users/Scope closure.
        this.finish = None;
        Poll::Ready(result)
    }
}

#[derive(Debug)]
struct PeerStopped(quinn::VarInt);
impl std::fmt::Display for PeerStopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "QUIC peer stopped stream: {}", self.0)
    }
}
impl std::error::Error for PeerStopped {}
