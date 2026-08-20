use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_traits::AsyncSocket;

pub use zero_platform_tokio::{ClientStream, PrefixedSocket, RelayCarrier, TcpRelayStream};

pub struct RecordingStream<S> {
    inner: S,
    recorded: Vec<u8>,
}

pub struct ReplayStream<S> {
    inner: S,
    prefix: Vec<u8>,
    offset: usize,
}

impl<S> ReplayStream<S> {
    pub fn new(inner: S, prefix: Vec<u8>) -> Self {
        Self {
            inner,
            prefix,
            offset: 0,
        }
    }
}

impl<S> AsyncRead for ReplayStream<S>
where
    S: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.offset < self.prefix.len() && buf.remaining() > 0 {
            let available = &self.prefix[self.offset..];
            let count = available.len().min(buf.remaining());
            buf.put_slice(&available[..count]);
            self.offset += count;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S> AsyncWrite for ReplayStream<S>
where
    S: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl<S> RecordingStream<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            recorded: Vec::with_capacity(128),
        }
    }

    pub fn into_parts(self) -> (S, Vec<u8>) {
        (self.inner, self.recorded)
    }
}

impl<S> AsyncRead for RecordingStream<S>
where
    S: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let prev = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &result {
            let n = buf.filled().len() - prev;
            if n > 0 {
                self.recorded.extend_from_slice(&buf.filled()[prev..]);
            }
        }
        result
    }
}

impl<S> AsyncWrite for RecordingStream<S>
where
    S: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl<S> AsyncSocket for RecordingStream<S>
where
    S: AsyncSocket<Error = io::Error> + Send + Sync,
{
    type Error = io::Error;

    fn transport_bypass_control(&self) -> Option<zero_traits::TransportBypassControl> {
        self.inner.transport_bypass_control()
    }

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        let n = self.inner.read(buf).await?;
        self.recorded.extend_from_slice(&buf[..n]);
        Ok(n)
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        self.inner.write_all(buf).await
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        self.inner.shutdown().await
    }
}

impl<S> ClientStream for RecordingStream<S>
where
    S: ClientStream + Send + Sync,
{
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner.peer_addr()
    }
}

impl<S> AsyncSocket for ReplayStream<S>
where
    S: AsyncSocket<Error = io::Error> + Send + Sync,
{
    type Error = io::Error;

    fn transport_bypass_control(&self) -> Option<zero_traits::TransportBypassControl> {
        self.inner.transport_bypass_control()
    }

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        if self.offset < self.prefix.len() {
            let available = &self.prefix[self.offset..];
            let count = available.len().min(buf.len());
            buf[..count].copy_from_slice(&available[..count]);
            self.offset += count;
            return Ok(count);
        }
        self.inner.read(buf).await
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        self.inner.write_all(buf).await
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        self.inner.shutdown().await
    }
}

impl<S> ClientStream for ReplayStream<S>
where
    S: ClientStream + Send + Sync,
{
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner.peer_addr()
    }
}
