//! Live observation of a stream carrier boundary, including partial writes.
use crate::ClientStream;
use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_traits::{AsyncSocket, IoObserver};

pub struct ObservedStream<S> {
    inner: S,
    observer: Arc<dyn IoObserver>,
}
impl<S> ObservedStream<S> {
    pub fn new(inner: S, observer: Arc<dyn IoObserver>) -> Self {
        observer.stream_boundary();
        Self { inner, observer }
    }
}
impl<S: AsyncRead + Unpin> AsyncRead for ObservedStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        match &result {
            Poll::Ready(Ok(())) => self.observer.received(buf.filled().len() - before),
            Poll::Ready(Err(_)) => self.observer.error(),
            Poll::Pending => {}
        }
        result
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for ObservedStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write(cx, buf);
        match &result {
            Poll::Ready(Ok(size)) => self.observer.sent(*size),
            Poll::Ready(Err(_)) => self.observer.error(),
            Poll::Pending => {}
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
impl<S: ClientStream> AsyncSocket for ObservedStream<S> {
    type Error = io::Error;
    fn transport_bypass_control(&self) -> Option<zero_traits::TransportBypassControl> {
        self.inner.transport_bypass_control()
    }
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        tokio::io::AsyncReadExt::read(self, buf).await
    }
    async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        tokio::io::AsyncWriteExt::write_all(self, buf).await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        tokio::io::AsyncWriteExt::shutdown(self).await
    }
}
impl<S: ClientStream> ClientStream for ObservedStream<S> {
    fn application_settings(&self) -> Option<&zero_platform_tokio::ApplicationSettings> {
        self.inner.application_settings()
    }
    fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.inner.local_addr()
    }
    fn peer_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.inner.peer_addr()
    }
}
