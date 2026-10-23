//! The actual transferred stack stream carries its same-graph view token.
use super::FlowUser;
use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub(in crate::runtime::packet_flow) struct TrackedStream<S> {
    // Rust drops fields in declaration order: release the original stream
    // before publishing that this transferred view is gone.
    stream: S,
    _user: FlowUser,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send + Sync>>,
}
impl<S> TrackedStream<S> {
    pub(in crate::runtime::packet_flow) fn new(stream: S, user: FlowUser) -> Self {
        let cancelled = Box::pin(user.owner.cancellation().clone().cancelled_owned());
        Self {
            stream,
            _user: user,
            cancelled,
        }
    }
    fn check(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            Err(io::Error::new(
                // Retirement is terminal. AsyncReadExt retries Interrupted,
                // which would spin forever after this cancellation completes.
                io::ErrorKind::BrokenPipe,
                "packet endpoint retired",
            ))
        } else {
            Ok(())
        }
    }
}
impl<S: AsyncRead + Unpin> AsyncRead for TrackedStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.check(cx)?;
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for TrackedStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.check(cx)?;
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check(cx)?;
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Preserve the original stream's half-close even during retirement.
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
