//! One reusable physical connection with one byte of non-consuming lookahead.
//! Keep-alive idleness is not a partial HTTP handshake. The buffer stays with
//! the original stream across messages and is handed off once on CONNECT/upgrade.
use alloc::boxed::Box;
use std::{
    io,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf,
};
use zero_platform_tokio::contract::stream::BoxStream;

pub(super) struct Connection(Mutex<BufReader<BoxStream>>);
impl Connection {
    pub(super) fn new(io: BoxStream) -> Self {
        Self(Mutex::new(BufReader::with_capacity(1, io)))
    }
    /// Cancellation-safe: fill_buf never consumes the first byte.
    pub(super) async fn request_started(&mut self) -> io::Result<bool> {
        Ok(!self.stream().fill_buf().await?.is_empty())
    }
    pub(super) fn into_stream(self) -> BoxStream {
        // Keep the original lookahead byte when CONNECT/Upgrade transfers custody.
        Box::pin(self)
    }
    fn stream(&mut self) -> &mut BufReader<BoxStream> {
        // The wire trait requires Sync; exclusive polling never locks or spawns.
        self.0.get_mut().expect("exclusive HTTP framing access")
    }
}
impl AsyncRead for Connection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.stream()).poll_read(cx, buf)
    }
}
impl AsyncWrite for Connection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.stream()).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.stream()).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.stream()).poll_shutdown(cx)
    }
}
impl zero_traits::AsyncSocket for Connection {
    type Error = io::Error;
    async fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        AsyncReadExt::read(self, bytes).await
    }
    async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        AsyncWriteExt::write_all(self, bytes).await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        AsyncWriteExt::shutdown(self).await
    }
}
