//! Cancellation and error propagation for HTTP-carried logical streams.
use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf},
    sync::Notify,
    task::AbortHandle,
};

#[derive(Default)]
pub(super) struct Lifetime {
    error: Mutex<Option<String>>,
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
    pub(super) fn error(&self) -> io::Result<()> {
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
    pub(super) life: Arc<Lifetime>,
    pub(super) usages: Vec<Arc<Mutex<super::xmux::Usage>>>,
    pub(super) tasks: Vec<AbortHandle>,
    pub(super) drain_on_drop: bool,
}
impl Drop for XhttpStream {
    fn drop(&mut self) {
        if self.drain_on_drop {
            return;
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
