use super::PoolUse;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Exclusive poll access makes a Send-only pinned carrier usable by existing
/// Sync transport wrappers, without a pipe, worker or second native owner.
pub struct ExclusiveIo<S>(std::sync::Mutex<S>);
impl<S> ExclusiveIo<S> {
    pub fn new(io: S) -> Self {
        Self(std::sync::Mutex::new(io))
    }
}
impl<S: AsyncRead + Unpin> AsyncRead for ExclusiveIo<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().0.get_mut().expect("exclusive carrier")).poll_read(cx, buf)
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for ExclusiveIo<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.get_mut().0.get_mut().expect("exclusive carrier")).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().0.get_mut().expect("exclusive carrier")).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().0.get_mut().expect("exclusive carrier")).poll_shutdown(cx)
    }
}

/// Split handles keep the same logical custody lease. It is independent of
/// the original carrier controller, its actual driver join and native proof.
pub struct UserStream<S> {
    io: S,
    _usage: PoolUse,
}
impl<S> UserStream<S> {
    pub fn new(io: S, usage: PoolUse) -> Self {
        Self { io, _usage: usage }
    }
}
impl<S: AsyncRead + Unpin> AsyncRead for UserStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_read(cx, buf)
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for UserStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}
