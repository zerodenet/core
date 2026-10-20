use super::slot::Slot;
use crate::contract::stream::BoxStream;
use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
pub struct CarrierIo {
    pub(super) io: BoxStream,
    pub(super) slot: Arc<Slot>,
}
impl AsyncRead for CarrierIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.io.as_mut().poll_read(cx, buf)
    }
}
impl AsyncWrite for CarrierIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.io.as_mut().poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.io.as_mut().poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.io.as_mut().poll_shutdown(cx)
    }
}
impl Drop for CarrierIo {
    fn drop(&mut self) {
        self.slot.state().view_live = false;
        self.slot.cancel.cancel();
        self.slot.notify();
    }
}

/// Opaque origin tag retained through the protocol's own preparation. This
/// prevents accidental cross-slot adoption; callbacks still obey their normal
/// cooperation contract and must retain the actual input carrier.
pub struct CarrierDriver<I> {
    pub(super) io: I,
    pub(super) slot: Arc<Slot>,
}
impl CarrierIo {
    pub fn into_driver(self) -> CarrierDriver<Self> {
        CarrierDriver {
            slot: self.slot.clone(),
            io: self,
        }
    }
    pub async fn prepare_driver<I, E, F, Fut>(self, prepare: F) -> Result<CarrierDriver<I>, E>
    where
        F: FnOnce(Self) -> Fut,
        Fut: std::future::Future<Output = Result<I, E>>,
    {
        let slot = self.slot.clone();
        prepare(self).await.map(|io| CarrierDriver { io, slot })
    }
}
impl<I> CarrierDriver<I> {
    /// Split protocol metadata from its prepared driver without changing origin.
    pub fn split<R, J>(self, split: impl FnOnce(I) -> (R, J)) -> (R, CarrierDriver<J>) {
        let (metadata, io) = split(self.io);
        (
            metadata,
            CarrierDriver {
                io,
                slot: self.slot,
            },
        )
    }
}
