use super::*;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_platform_tokio::contract::{stream::BoxStream, BoxFuture};

pub(super) struct Transferred {
    // Drop the actual protocol stream before releasing the user census entry.
    pub(super) io: BoxStream,
    pub(super) _lease: Arc<Lease>,
    pub(super) revoked: BoxFuture<'static, ()>,
}
impl Transferred {
    fn check(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self._lease.is_revoked() || self.revoked.as_mut().poll(cx).is_ready() {
            Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "component resource retired",
            ))
        } else {
            Ok(())
        }
    }
}
impl AsyncRead for Transferred {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.check(cx)?;
        this.io.as_mut().poll_read(cx, buf)
    }
}
impl AsyncWrite for Transferred {
    fn is_write_vectored(&self) -> bool {
        self.io.as_ref().get_ref().is_write_vectored()
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.check(cx)?;
        this.io.as_mut().poll_write_vectored(cx, bufs)
    }
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.check(cx)?;
        this.io.as_mut().poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.check(cx)?;
        this.io.as_mut().poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().io.as_mut().poll_shutdown(cx)
    }
}
