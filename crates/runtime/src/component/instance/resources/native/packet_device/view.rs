//! The unique I/O view returns custody only; Drop is never a native close ACK.
use super::*;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_platform_tokio::contract::component::PacketDeviceIo;
pub(super) struct View {
    io: Option<PacketDeviceIo>,
    state: Arc<Mutex<State>>,
}
impl View {
    pub(super) fn new(io: PacketDeviceIo, state: Arc<Mutex<State>>) -> Self {
        Self {
            io: Some(io),
            state,
        }
    }
}
impl Drop for View {
    fn drop(&mut self) {
        drop(self.io.take());
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .view_live = false;
    }
}
impl AsyncRead for View {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.get_mut()
            .io
            .as_mut()
            .expect("live unique Packet view")
            .as_mut()
            .poll_read(cx, buffer)
    }
}
impl AsyncWrite for View {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut()
            .io
            .as_mut()
            .expect("live unique Packet view")
            .as_mut()
            .poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut()
            .io
            .as_mut()
            .expect("live unique Packet view")
            .as_mut()
            .poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut()
            .io
            .as_mut()
            .expect("live unique Packet view")
            .as_mut()
            .poll_shutdown(cx)
    }
}
