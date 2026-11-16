//! Two existing cancellation fences around the actual returned Stream view.
use super::*;
use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_platform_tokio::contract::CancellationToken;
struct Stream {
    io: BoxStream,
    logical: CancellationToken,
    read_caller: Pin<Box<dyn Future<Output = ()> + Send>>,
    read_parent: Pin<Box<dyn Future<Output = ()> + Send>>,
    write_caller: Pin<Box<dyn Future<Output = ()> + Send>>,
    write_parent: Pin<Box<dyn Future<Output = ()> + Send>>,
}
pub(super) fn guard(
    io: BoxStream,
    logical: CancellationToken,
    caller: CancellationToken,
) -> BoxStream {
    Box::pin(Stream {
        io,
        read_parent: Box::pin(logical.clone().cancelled_owned()),
        read_caller: Box::pin(caller.clone().cancelled_owned()),
        write_parent: Box::pin(logical.clone().cancelled_owned()),
        write_caller: Box::pin(caller.cancelled_owned()),
        logical,
    })
}
impl Stream {
    fn check(&mut self, cx: &mut Context<'_>, write: bool) -> io::Result<()> {
        // Separate read/write registrations retain both real pollers after a
        // split; one direction must not replace the other's cancellation waker.
        let (parent, caller) = if write {
            (&mut self.write_parent, &mut self.write_caller)
        } else {
            (&mut self.read_parent, &mut self.read_caller)
        };
        if parent.as_mut().poll(cx).is_ready() || caller.as_mut().poll(cx).is_ready() {
            self.logical.cancel();
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "original prefix caller cancelled",
            ))
        } else {
            Ok(())
        }
    }
}
impl Drop for Stream {
    fn drop(&mut self) {
        self.logical.cancel();
    }
}
impl AsyncRead for Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Err(error) = self.check(cx, false) {
            return Poll::Ready(Err(error));
        }
        self.io.as_mut().poll_read(cx, buf)
    }
}
impl AsyncWrite for Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Err(error) = self.check(cx, true) {
            return Poll::Ready(Err(error));
        }
        self.io.as_mut().poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Err(error) = self.check(cx, true) {
            return Poll::Ready(Err(error));
        }
        self.io.as_mut().poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Err(error) = self.check(cx, true) {
            return Poll::Ready(Err(error));
        }
        self.io.as_mut().poll_shutdown(cx)
    }
}
