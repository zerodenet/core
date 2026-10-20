use super::*;
use std::{
    future::poll_fn,
    net::SocketAddr,
    task::{Context, Poll},
};
impl ControlledDatagramSocket {
    pub fn reservation(&self) -> DatagramReservationId {
        self.entry.id
    }
    pub fn egress_interface(&self) -> Option<&EgressInterface> {
        self.interface.as_ref()
    }
    pub fn local_addr(&self) -> std_io::Result<SocketAddr> {
        match &*self.entry.lock() {
            NativeHandleState::Live(socket) => socket.local_addr(),
            NativeHandleState::Raw(socket) => socket.local_addr(),
            other => Err(unavailable(other)),
        }
    }
    /// In-place observation keeps the original in the persistent owner even
    /// if an external boundary observer panics.
    pub fn set_observer(
        &mut self,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> std_io::Result<()> {
        match &mut *self.entry.lock() {
            NativeHandleState::Live(socket) => {
                socket.set_observer(observer);
                Ok(())
            }
            other => Err(unavailable(other)),
        }
    }
    pub fn poll_send_to_addr(
        &self,
        cx: &mut Context<'_>,
        bytes: &[u8],
        peer: SocketAddr,
    ) -> Poll<std_io::Result<usize>> {
        match &*self.entry.lock() {
            NativeHandleState::Live(socket) => socket.poll_send_to_addr(cx, bytes, peer),
            other => Poll::Ready(Err(unavailable(other))),
        }
    }
    pub fn poll_recv_from_addr(
        &self,
        cx: &mut Context<'_>,
        bytes: &mut [u8],
    ) -> Poll<std_io::Result<(usize, SocketAddr)>> {
        match &*self.entry.lock() {
            NativeHandleState::Live(socket) => socket.poll_recv_from_addr(cx, bytes),
            other => Poll::Ready(Err(unavailable(other))),
        }
    }
    pub async fn send_to_addr(&self, bytes: &[u8], peer: SocketAddr) -> std_io::Result<usize> {
        poll_fn(|cx| self.poll_send_to_addr(cx, bytes, peer)).await
    }
    pub async fn recv_from_addr(&self, bytes: &mut [u8]) -> std_io::Result<(usize, SocketAddr)> {
        poll_fn(|cx| self.poll_recv_from_addr(cx, bytes)).await
    }
    /// The sole I/O owner invokes this after retiring its old poll/mappings.
    /// The actual result is retained before returning a reusable-slot receipt.
    pub fn close(&self) -> Result<DatagramCloseReceipt, Arc<std_io::Error>> {
        self.entry.close()
    }
}
pub(super) fn unavailable(native: &NativeHandleState) -> std_io::Error {
    match native {
        NativeHandleState::Unknown(source) => std_io::Error::other(source.clone()),
        _ => std_io::Error::new(
            std_io::ErrorKind::NotConnected,
            "original UDP socket is not live",
        ),
    }
}
