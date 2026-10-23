use std::{
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
pub(super) struct PendingRead {
    socket: tokio::net::TcpStream,
    observed: Option<Arc<tokio::sync::Notify>>,
}
impl PendingRead {
    pub(super) fn new(
        socket: tokio::net::TcpStream,
        observed: Option<Arc<tokio::sync::Notify>>,
    ) -> Self {
        Self { socket, observed }
    }
}
impl AsyncRead for PendingRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let result = Pin::new(&mut self.socket).poll_read(cx, buffer);
        if result.is_pending() {
            if let Some(observed) = &self.observed {
                observed.notify_one();
            }
        }
        result
    }
}
impl AsyncWrite for PendingRead {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.socket).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}
#[derive(Debug)]
pub(crate) struct OriginalFailure(pub(crate) Arc<()>);
impl std::fmt::Display for OriginalFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original native Mieru transport failure")
    }
}
impl std::error::Error for OriginalFailure {}
#[derive(Default)]
pub(crate) struct FailureControl {
    failed: AtomicBool,
    waker: Mutex<Option<Waker>>,
}
impl FailureControl {
    pub(crate) fn trigger(&self) {
        self.failed.store(true, Ordering::SeqCst);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}
pub(super) struct FailRead {
    socket: tokio::net::TcpStream,
    control: Arc<FailureControl>,
    marker: Arc<()>,
}
impl FailRead {
    pub(super) fn new(
        socket: tokio::net::TcpStream,
        control: Arc<FailureControl>,
        marker: Arc<()>,
    ) -> Self {
        Self {
            socket,
            control,
            marker,
        }
    }
}
impl AsyncRead for FailRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        *self.control.waker.lock().unwrap() = Some(cx.waker().clone());
        if self.control.failed.load(Ordering::SeqCst) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                OriginalFailure(self.marker.clone()),
            )));
        }
        Pin::new(&mut self.socket).poll_read(cx, buffer)
    }
}
impl AsyncWrite for FailRead {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.socket).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}
