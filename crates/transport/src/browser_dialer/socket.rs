use super::IdleConnection;
use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use zero_platform_tokio::ClientStream;
use zero_traits::AsyncSocket;

/// A browser-backed byte stream. Each instance owns exactly one authenticated
/// local control WebSocket, so replies cannot be confused across requests.
/// Its resource permit remains held for the full task lifetime.
pub struct BrowserStream {
    inner: crate::ws::WebSocketSocket<zero_platform_tokio::PrefixedSocket>,
    _permit: tokio::sync::OwnedSemaphorePermit,
    cancelled: Pin<Box<tokio_util::sync::WaitForCancellationFutureOwned>>,
}

impl BrowserStream {
    pub(super) fn new(connection: IdleConnection, heartbeat_period_secs: u32) -> Self {
        let mut inner = crate::ws::WebSocketSocket::new(connection.socket);
        inner.set_heartbeat(heartbeat_period_secs);
        Self {
            inner,
            _permit: connection._permit,
            cancelled: Box::pin(connection.cancellation.cancelled_owned()),
        }
    }

    fn poll_cancelled(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "Browser Dialer retired",
            ))
        } else {
            Ok(())
        }
    }
}

impl AsyncRead for BrowserStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.poll_cancelled(cx)?;
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for BrowserStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_cancelled(cx)?;
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_cancelled(cx)?;
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_cancelled(cx)?;
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl AsyncSocket for BrowserStream {
    type Error = io::Error;

    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        AsyncReadExt::read(self, buf).await
    }

    async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        AsyncWriteExt::write_all(self, buf).await?;
        AsyncWriteExt::flush(self).await
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        AsyncWriteExt::shutdown(self).await
    }
}

impl ClientStream for BrowserStream {}
