//! Exclusive polling of the original pinned carrier, with its original I/O error.
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
use zero_platform_tokio::contract::stream::BoxStream;
#[derive(Clone)]
pub(super) struct OriginalIo(Arc<io::Error>);
impl fmt::Debug for OriginalIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}
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
#[derive(Clone, Default)]
pub(super) struct Observed {
    written: Arc<AtomicU64>,
    error: Arc<Mutex<Option<OriginalIo>>>,
}
impl Observed {
    pub(super) fn written(&self) -> u64 {
        self.written.load(Ordering::Relaxed)
    }
    pub(super) fn error(&self) -> Option<OriginalIo> {
        self.error
            .lock()
            .expect("original Shadowsocks IO source")
            .take()
    }
    fn capture(&self, error: io::Error) -> io::Error {
        let kind = error.kind();
        let original = OriginalIo(Arc::new(error));
        *self.error.lock().expect("original Shadowsocks IO source") = Some(original.clone());
        // The actual error object stays in the source chain, including private
        // payloads and raw OS details. It is never reconstructed from raw_os_error.
        io::Error::new(kind, original)
    }
}
pub(super) struct Carrier {
    io: Mutex<BoxStream>,
    observed: Observed,
}
impl Carrier {
    pub(super) fn new(io: BoxStream) -> (Self, Observed) {
        let observed = Observed::default();
        (
            Self {
                io: Mutex::new(io),
                observed: observed.clone(),
            },
            observed,
        )
    }
}
impl AsyncRead for Carrier {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.io
            .get_mut()
            .expect("exclusive carrier")
            .as_mut()
            .poll_read(cx, buf)
            .map_err(|source| this.observed.capture(source))
    }
}
impl AsyncWrite for Carrier {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match this
            .io
            .get_mut()
            .expect("exclusive carrier")
            .as_mut()
            .poll_write(cx, buf)
        {
            Poll::Ready(Ok(size)) => {
                this.observed
                    .written
                    .fetch_add(size as u64, Ordering::Relaxed);
                Poll::Ready(Ok(size))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(this.observed.capture(error))),
            Poll::Pending => Poll::Pending,
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.io
            .get_mut()
            .expect("exclusive carrier")
            .as_mut()
            .poll_flush(cx)
            .map_err(|source| this.observed.capture(source))
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.io
            .get_mut()
            .expect("exclusive carrier")
            .as_mut()
            .poll_shutdown(cx)
            .map_err(|source| this.observed.capture(source))
    }
}
impl zero_traits::AsyncSocket for Carrier {
    type Error = io::Error;
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        AsyncReadExt::read(self, buf).await
    }
    async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        if let Err(source) = AsyncWriteExt::write_all(self, buf).await {
            let captured = self
                .observed
                .error
                .lock()
                .expect("original Shadowsocks IO source")
                .is_some();
            return Err(if captured {
                source
            } else {
                self.observed.capture(source)
            });
        }
        AsyncWriteExt::flush(self).await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        AsyncWriteExt::shutdown(self).await
    }
}

#[cfg(test)]
#[path = "../../../tests/component/tcp/carrier.rs"]
mod tests;
