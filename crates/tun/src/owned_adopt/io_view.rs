//! Unique I/O view; dropping it does not release the persistent original owner.
use super::*;
use std::{
    net::IpAddr,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub struct AdoptedTunIo {
    pub(super) owner: Arc<AdoptedTunOwner>,
    pub(super) identity: OwnedTunIdentity,
    pub(super) name: String,
}
impl AdoptedTunIo {
    pub fn owned_identity(&self) -> &OwnedTunIdentity {
        &self.identity
    }
}
fn unavailable() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, "owned host device is not live")
}
impl AsyncRead for AdoptedTunIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self.owner.lock() {
            State::Live { device, identity } if identity.same_owner(&self.identity) => {
                Pin::new(device).poll_read(cx, buffer)
            }
            _ => Poll::Ready(Err(unavailable())),
        }
    }
}
impl AsyncWrite for AdoptedTunIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self.owner.lock() {
            State::Live { device, identity } if identity.same_owner(&self.identity) => {
                Pin::new(device).poll_write(cx, bytes)
            }
            _ => Poll::Ready(Err(unavailable())),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self.owner.lock() {
            State::Live { device, identity } if identity.same_owner(&self.identity) => {
                Pin::new(device).poll_flush(cx)
            }
            _ => Poll::Ready(Err(unavailable())),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self.owner.lock() {
            State::Live { device, identity } if identity.same_owner(&self.identity) => {
                Pin::new(device).poll_shutdown(cx)
            }
            _ => Poll::Ready(Err(unavailable())),
        }
    }
}
impl crate::TunDevice for AdoptedTunIo {
    fn configure(&self, _: IpAddr, _: IpAddr, _: u16) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "host-owned TUN configuration is read-only",
        ))
    }
    fn name(&self) -> &str {
        &self.name
    }
}
