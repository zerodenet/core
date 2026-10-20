//! Exercise the actual TLS stack with a pinned, non-Sync physical carrier.
use std::{
    cell::Cell,
    io,
    marker::PhantomPinned,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_platform_tokio::contract::stream::BoxStream;

struct PinnedCarrier {
    io: Mutex<BoxStream>,
    _pin: PhantomPinned,
    _not_sync: Cell<()>,
}
pub(super) fn wrap(io: BoxStream) -> BoxStream {
    Box::pin(PinnedCarrier {
        io: Mutex::new(io),
        _pin: PhantomPinned,
        _not_sync: Cell::new(()),
    })
}
impl AsyncRead for PinnedCarrier {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.as_ref()
            .get_ref()
            .io
            .lock()
            .unwrap()
            .as_mut()
            .poll_read(cx, buffer)
    }
}
impl AsyncWrite for PinnedCarrier {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.as_ref()
            .get_ref()
            .io
            .lock()
            .unwrap()
            .as_mut()
            .poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.as_ref()
            .get_ref()
            .io
            .lock()
            .unwrap()
            .as_mut()
            .poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.as_ref()
            .get_ref()
            .io
            .lock()
            .unwrap()
            .as_mut()
            .poll_shutdown(cx)
    }
}
