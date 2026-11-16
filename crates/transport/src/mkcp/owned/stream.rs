//! The user permit outlives the real stream, including both split halves.
use super::*;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_platform_tokio::ClientStream;
pub(super) struct View {
    pub io: super::super::MkcpStream,
    pub _user: graph::User,
    pub _memory: budget::Memory,
}
impl AsyncRead for View {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        dst: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, dst)
    }
}
impl AsyncWrite for View {
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
impl ClientStream for View {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.local_addr()
    }
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.io.peer_addr()
    }
}
impl zero_traits::AsyncSocket for View {
    type Error = io::Error;
    async fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        tokio::io::AsyncReadExt::read(self, bytes).await
    }
    async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        tokio::io::AsyncWriteExt::write_all(self, bytes).await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        tokio::io::AsyncWriteExt::shutdown(self).await
    }
}
