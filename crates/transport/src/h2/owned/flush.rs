//! Actual underlying flush observation for this same original H2 connection.
use super::*;
pub(in crate::h2) use crate::native_flush::Flushed;
pub(in crate::h2) struct FlushIo<S> {
    pub(in crate::h2) io: S,
    pub(in crate::h2) flush: crate::native_flush::NativeFlush,
    pub(in crate::h2) read: Option<Arc<super::read::ReadSignal>>,
}
impl<S: AsyncRead + Unpin> AsyncRead for FlushIo<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.flush.bind(cx);
        let Some(read) = this.read.as_ref().filter(|read| read.armed()) else {
            return Pin::new(&mut this.io).poll_read(cx, out);
        };
        if !read.bind(cx.waker()) {
            // A migrated context disables this scheduling optimization; use
            // that real current caller's wake and the normal full driver.
            return Pin::new(&mut this.io).poll_read(cx, out);
        }
        read.before_read();
        let wake = futures_util::task::waker_ref(read);
        let mut native = Context::from_waker(&wake);
        let result = Pin::new(&mut this.io).poll_read(&mut native, out);
        if result.is_ready() {
            read.read_ready();
        }
        result
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for FlushIo<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.flush.bind(cx);
        Pin::new(&mut this.io).poll_write(cx, bytes)
    }
    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.flush.bind(cx);
        Pin::new(&mut this.io).poll_write_vectored(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.flush.poll(&mut this.io, cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.flush.bind(cx);
        Pin::new(&mut this.io).poll_shutdown(cx)
    }
}
