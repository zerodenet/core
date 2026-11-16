//! These actual duplex streams deliberately cannot satisfy ClientStream/Sync.
//! Existing cancellation, keep-alive and upgrade regressions consume them directly.
use super::*;
use std::cell::Cell;
use zero_platform_tokio::contract::context::InboundConnectionFacts;

pub(super) fn source(io: tokio::io::DuplexStream) -> Requests {
    Requests::from_stream(
        Box::pin(SendOnly::new(io)),
        InboundConnectionFacts::default(),
    )
}
pub(super) struct SendOnly<S> {
    io: S,
    _exclusive: Cell<()>,
}
impl<S> SendOnly<S> {
    pub(super) fn new(io: S) -> Self {
        Self {
            io,
            _exclusive: Cell::new(()),
        }
    }
}
impl<S: AsyncRead + Unpin> AsyncRead for SendOnly<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for SendOnly<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
