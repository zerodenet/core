use super::IdleConnection;
use std::{
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
    inner: crate::ws::WebSocketSocket<super::wire::Socket>,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
    cancelled: tokio_util::sync::CancellationToken,
    original: Option<super::original::BrowserUse>,
    admission: std::sync::Mutex<Option<super::memory::Wait>>,
    memory: super::memory::Memory,
    borrowed: Option<Borrowed>,
}
struct Borrowed {
    future: Pin<Box<tokio_util::sync::WaitForCancellationFutureOwned>>,
    _header: zero_traits::packet_metadata::PacketMetadataCredit,
}

impl BrowserStream {
    pub(super) fn new(connection: IdleConnection, heartbeat_period_secs: u32) -> Self {
        let mut inner = crate::ws::WebSocketSocket::new(connection.socket);
        inner.set_heartbeat(heartbeat_period_secs);
        Self {
            inner,
            _permit: connection._permit,
            original: connection.original,
            admission: std::sync::Mutex::new(None),
            memory: connection.memory,
            cancelled: connection.cancellation,
            borrowed: None,
        }
    }

    pub fn original(&self) -> Option<&super::BrowserOriginal> {
        self.original.as_ref().map(|value| &value.0)
    }

    pub(super) fn bind_borrowed(
        mut self,
        cancel: tokio_util::sync::CancellationToken,
        header: zero_traits::packet_metadata::PacketMetadataCredit,
    ) -> io::Result<Self> {
        let needed = zero_traits::packet_metadata::PacketMetadataLayout::boxed::<
            tokio_util::sync::WaitForCancellationFutureOwned,
        >();
        if header.layout().bytes() < needed.bytes()
            || header.layout().allocations() < needed.allocations()
        {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        if self.borrowed.is_some() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        self.borrowed = Some(Borrowed {
            future: Box::pin(cancel.cancelled_owned()),
            _header: header,
        });
        Ok(self)
    }
    fn poll_cancelled(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self
            .borrowed
            .as_mut()
            .is_some_and(|borrowed| borrowed.future.as_mut().poll(cx).is_ready())
        {
            return Err(io::ErrorKind::ConnectionAborted.into());
        }
        if self.cancelled.is_cancelled() {
            Err(io::Error::new(
                // Retirement is permanent. I/O helpers retry Interrupted;
                // repeating it can spin inside one poll and starve deadlines.
                io::ErrorKind::ConnectionAborted,
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
        let peak = buf
            .len()
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(512 * 1024))
            .ok_or(io::ErrorKind::InvalidInput)?;
        loop {
            let pending = self
                .admission
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some(mut wait) = pending {
                match wait.future.as_mut().poll(cx) {
                    Poll::Pending => {
                        *self
                            .admission
                            .get_mut()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(wait);
                        return Poll::Pending;
                    }
                    Poll::Ready(result) => result?,
                }
            }
            match self.memory.transmit_growth(peak)? {
                Some(wait) => {
                    *self
                        .admission
                        .get_mut()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(wait);
                }
                None => break,
            }
        }
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
