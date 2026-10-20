//! The sole poll borrow supplies Sync without duplicating or piping the fd.
use std::{
    io,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_platform_tokio::controlled_carrier::CarrierIo;
pub(super) struct Exclusive(Mutex<CarrierIo>);
impl Exclusive {
    pub(super) fn new(io: CarrierIo) -> Self {
        Self(Mutex::new(io))
    }
}
impl AsyncRead for Exclusive {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.0.get_mut().unwrap()).poll_read(cx, buffer)
    }
}
impl AsyncWrite for Exclusive {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.0.get_mut().unwrap()).poll_write(cx, data)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.0.get_mut().unwrap()).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.0.get_mut().unwrap()).poll_shutdown(cx)
    }
}
