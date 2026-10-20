//! An exclusive poll borrow of the original pinned carrier, without a pipe/task.
use std::{
    io,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_platform_tokio::contract::stream::BoxStream;

pub(super) struct Carrier(Mutex<BoxStream>);
impl Carrier {
    pub(super) fn new(stream: BoxStream) -> Self {
        Self(Mutex::new(stream))
    }
}
impl AsyncRead for Carrier {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.get_mut()
            .0
            .get_mut()
            .expect("exclusive carrier")
            .as_mut()
            .poll_read(cx, buf)
    }
}
impl AsyncWrite for Carrier {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut()
            .0
            .get_mut()
            .expect("exclusive carrier")
            .as_mut()
            .poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut()
            .0
            .get_mut()
            .expect("exclusive carrier")
            .as_mut()
            .poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut()
            .0
            .get_mut()
            .expect("exclusive carrier")
            .as_mut()
            .poll_shutdown(cx)
    }
}
