//! Cancellation and error propagation for HTTP-carried logical streams.
use std::{
    io,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf},
    sync::Notify,
    task::AbortHandle,
};

mod lifetime;
pub(super) use lifetime::LifetimeRef;

#[derive(Default)]
pub(super) struct Lifetime {
    error: Mutex<Option<String>>,
    #[cfg(feature = "quic")]
    retained_error: std::sync::OnceLock<zero_traits::packet_metadata::PacketMetadataError>,
    closed: std::sync::atomic::AtomicBool,
    wake: Notify,
    accepted: std::sync::atomic::AtomicU64,
    committed: std::sync::atomic::AtomicU64,
    flush_waker: futures_util::task::AtomicWaker,
}
impl Lifetime {
    pub(super) fn commit(&self, count: usize) {
        self.committed
            .fetch_add(count as u64, std::sync::atomic::Ordering::Release);
        self.flush_waker.wake();
    }
    fn poll_flush(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.error()?;
        self.flush_waker.register(cx.waker());
        if self.committed.load(std::sync::atomic::Ordering::Acquire)
            >= self.accepted.load(std::sync::atomic::Ordering::Acquire)
        {
            Poll::Ready(Ok(()))
        } else if self.is_closed() {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "xhttp upload closed before flush",
            )))
        } else {
            Poll::Pending
        }
    }
    pub(super) fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::Acquire)
    }
    pub(super) fn close(&self) {
        self.closed
            .store(true, std::sync::atomic::Ordering::Release);
        self.wake.notify_waiters();
        self.flush_waker.wake();
    }
    pub(super) fn fail(&self, error: impl std::fmt::Display) {
        *self.error.lock().unwrap() = Some(error.to_string());
        self.close();
    }
    #[cfg(feature = "quic")]
    pub(super) fn fail_retained(&self, source: zero_traits::packet_metadata::PacketMetadataError) {
        let _ = self.retained_error.set(source);
        self.close();
    }
    pub(super) fn error(&self) -> io::Result<()> {
        #[cfg(feature = "quic")]
        if let Some(source) = self.retained_error.get() {
            let kind = source
                .as_error()
                .downcast_ref::<io::Error>()
                .map_or(io::ErrorKind::Other, io::Error::kind);
            return Err(io::Error::new(kind, source.clone()));
        }
        match self.error.lock().unwrap().as_ref() {
            Some(error) => Err(io::Error::other(error.clone())),
            None => Ok(()),
        }
    }
    pub(super) async fn cancelled(&self) {
        loop {
            let notified = self.wake.notified();
            if self.closed.load(std::sync::atomic::Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

pub struct XhttpStream {
    pub(super) inner: DuplexStream,
    pub(super) owned_return: Option<super::xmux::owned::OwnedLogicalReturn>,
    pub(super) life: LifetimeRef,
    pub(super) usages: Vec<super::xmux::UsageRef>,
    pub(super) tasks: Vec<AbortHandle>,
    pub(super) drain_on_drop: bool,
    pub(super) _owned_usage_memory: Option<zero_traits::packet_metadata::PacketMetadataCredit>,
    pub(super) _owned_buffer: Option<zero_platform_tokio::controlled_carrier::CarrierBufferCredit>,
}
impl XhttpStream {
    pub fn owned_return(&self) -> Option<super::xmux::owned::OwnedLogicalReturn> {
        self.owned_return.clone()
    }
}
impl Drop for XhttpStream {
    fn drop(&mut self) {
        if self.drain_on_drop {
            return;
        }
        if let Some(original) = &self.owned_return {
            original.request_return();
        }
        self.life.close();
        for task in &self.tasks {
            task.abort();
        }
    }
}
impl AsyncRead for XhttpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.life.error()?;
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl AsyncWrite for XhttpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.life.error()?;
        let result = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(count)) = result {
            if !self.drain_on_drop {
                self.life
                    .accepted
                    .fetch_add(count as u64, std::sync::atomic::Ordering::Release);
            }
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.life.error()?;
        if self.drain_on_drop {
            Pin::new(&mut self.inner).poll_flush(cx)
        } else {
            self.life.poll_flush(cx)
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        std::task::ready!(Pin::new(&mut self.inner).poll_shutdown(cx))?;
        if self.drain_on_drop {
            Poll::Ready(Ok(()))
        } else {
            self.life.poll_flush(cx)
        }
    }
}
impl zero_traits::AsyncSocket for XhttpStream {
    type Error = io::Error;
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        tokio::io::AsyncReadExt::read(self, buf).await
    }
    async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        tokio::io::AsyncWriteExt::write_all(self, buf).await?;
        tokio::io::AsyncWriteExt::flush(self).await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        tokio::io::AsyncWriteExt::shutdown(self).await
    }
}
impl zero_platform_tokio::ClientStream for XhttpStream {}
