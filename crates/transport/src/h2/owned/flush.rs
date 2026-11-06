//! Actual underlying flush observation for this same original H2 connection.
use super::*;
#[derive(Default)]
pub(super) struct Flushed {
    pub(super) count: AtomicU64,
    pub(super) writer: AtomicWaker,
    pub(super) waiting: AtomicBool,
}
pub(super) struct FlushIo<S> {
    pub(super) io: S,
    pub(super) flush: Arc<Flushed>,
}
impl<S: AsyncRead + Unpin> AsyncRead for FlushIo<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_read(cx, out)
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for FlushIo<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(cx, bytes)
    }
    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write_vectored(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut this.io).poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                this.flush.count.fetch_add(1, Ordering::AcqRel);
                if this.flush.waiting.swap(false, Ordering::AcqRel) {
                    this.flush.writer.wake();
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}
