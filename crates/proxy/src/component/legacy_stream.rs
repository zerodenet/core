//! Temporary single conversion into the legacy relay facade. No pipe or task.
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_platform_tokio::{contract::stream::BoxStream, ClientStream};

pub(crate) struct LegacyStream {
    io: Mutex<BoxStream>,
    local: Option<SocketAddr>,
    peer: SocketAddr,
}
impl LegacyStream {
    pub(crate) fn new(io: BoxStream, local: Option<SocketAddr>, peer: SocketAddr) -> Self {
        Self {
            io: Mutex::new(io),
            local,
            peer,
        }
    }
    fn stream(&mut self) -> &mut BoxStream {
        // Exclusive polling access needs no mutex lock, and never holds a lock
        // over an await. Mutex only satisfies the old facade's Sync bound.
        self.io.get_mut().expect("stream has no shared lock access")
    }
}
impl AsyncRead for LegacyStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.stream().as_mut().poll_read(cx, buf)
    }
}
impl AsyncWrite for LegacyStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.stream().as_mut().poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.stream().as_mut().poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.stream().as_mut().poll_shutdown(cx)
    }
}
impl zero_traits::AsyncSocket for LegacyStream {
    type Error = io::Error;
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
impl ClientStream for LegacyStream {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.local.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "Direct local endpoint was not supplied",
            )
        })
    }
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.peer)
    }
}
