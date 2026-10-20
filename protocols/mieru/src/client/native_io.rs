//! Retain the actual I/O error across the legacy protocol's static Error::Io mapping.
use std::{
    error::Error,
    fmt, io,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use zero_traits::AsyncSocket;

#[derive(Clone, Default)]
pub(crate) struct Captured(Arc<Observation>);
#[derive(Default)]
struct Observation {
    error: Mutex<Option<Arc<io::Error>>>,
    written: AtomicU64,
}
pub(crate) struct Capture<S> {
    inner: Mutex<S>,
    errors: Captured,
}
#[derive(Debug)]
struct OriginalIo(Arc<io::Error>);
impl fmt::Display for OriginalIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}
impl Error for OriginalIo {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}
fn shared(error: Arc<io::Error>) -> io::Error {
    io::Error::new(error.kind(), OriginalIo(error))
}
impl Captured {
    pub(crate) fn written(&self) -> u64 {
        self.0.written.load(Ordering::Relaxed)
    }
    pub(crate) fn has_error(&self) -> bool {
        self.0.error.lock().unwrap().is_some()
    }
    fn record(&self, error: io::Error) -> io::Error {
        let error = Arc::new(error);
        let mut saved = self.0.error.lock().unwrap();
        if saved.is_none() {
            *saved = Some(error.clone());
        }
        shared(error)
    }
    pub(crate) fn recover(&self, fallback: io::Error) -> io::Error {
        self.0
            .error
            .lock()
            .unwrap()
            .as_ref()
            .map(|error| shared(error.clone()))
            .unwrap_or(fallback)
    }
    fn generated(&self, error: io::Error) -> io::Error {
        let empty = self.0.error.lock().unwrap().is_none();
        if empty {
            self.record(error)
        } else {
            self.recover(error)
        }
    }
}
impl<S> Capture<S> {
    pub(crate) fn new(inner: S) -> (Self, Captured) {
        let errors = Captured::default();
        (
            Self {
                inner: Mutex::new(inner),
                errors: errors.clone(),
            },
            errors,
        )
    }
}
impl<S: AsyncRead + Unpin> AsyncRead for Capture<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(
            self.inner
                .get_mut()
                .expect("exclusive native Mieru carrier"),
        )
        .poll_read(cx, buffer)
        .map_err(|error| self.errors.record(error))
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for Capture<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(
            self.inner
                .get_mut()
                .expect("exclusive native Mieru carrier"),
        )
        .poll_write(cx, bytes)
        .map_err(|error| self.errors.record(error));
        if let Poll::Ready(Ok(written)) = result {
            self.errors
                .0
                .written
                .fetch_add(written as u64, Ordering::Relaxed);
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(
            self.inner
                .get_mut()
                .expect("exclusive native Mieru carrier"),
        )
        .poll_flush(cx)
        .map_err(|error| self.errors.record(error))
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(
            self.inner
                .get_mut()
                .expect("exclusive native Mieru carrier"),
        )
        .poll_shutdown(cx)
        .map_err(|error| self.errors.record(error))
    }
}
impl<S: AsyncRead + AsyncWrite + Send + Unpin> AsyncSocket for Capture<S> {
    type Error = io::Error;
    async fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        AsyncReadExt::read(self, buffer).await
    }
    async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        if let Err(error) = AsyncWriteExt::write_all(self, bytes).await {
            return Err(self.errors.generated(error));
        }
        AsyncWriteExt::flush(self).await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        AsyncWriteExt::shutdown(self).await
    }
}
