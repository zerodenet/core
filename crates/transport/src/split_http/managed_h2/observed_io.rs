use super::{evidence::IoOperation, failure::Failure};
use std::{
    error::Error,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// A unique view of the same original IO; no descriptor duplication or piping.
pub(super) struct ObservedIo<S> {
    io: S,
    failure: Arc<Failure>,
}
impl<S> ObservedIo<S> {
    pub(super) fn new(io: S, failure: Arc<Failure>) -> Self {
        Self { io, failure }
    }
    fn capture<T>(
        &self,
        operation: IoOperation,
        result: Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        result.map(|result| {
            result.map_err(|error| {
                let original = Arc::new(error);
                self.failure.observe_io(operation, original.clone());
                io::Error::new(original.kind(), SharedIo(original))
            })
        })
    }
}
#[derive(Debug)]
struct SharedIo(Arc<io::Error>);
impl std::fmt::Display for SharedIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl Error for SharedIo {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}
impl<S: AsyncRead + Unpin> AsyncRead for ObservedIo<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.io).poll_read(cx, buffer);
        this.capture(IoOperation::Read, result)
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for ObservedIo<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.io).poll_write(cx, bytes);
        this.capture(IoOperation::Write, result)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.io).poll_flush(cx);
        this.capture(IoOperation::Flush, result)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.io).poll_shutdown(cx);
        this.capture(IoOperation::Shutdown, result)
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.io).poll_write_vectored(cx, buffers);
        this.capture(IoOperation::WriteVectored, result)
    }
    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }
}
