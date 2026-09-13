//! Object-safe polling boundary for datagram carriers with logical peer addresses.
use crate::SocketAddress;
use alloc::boxed::Box;
use core::task::{Context, Poll};
use core::{future::Future, pin::Pin};
pub trait PacketSocketIo: Send + Sync + Unpin {
    type Error;
    fn local_addr(&self) -> Result<SocketAddress, Self::Error>;
    fn poll_recv_from(
        &self,
        cx: &mut Context<'_>,
        bytes: &mut [u8],
    ) -> Poll<Result<(usize, SocketAddress), Self::Error>>;
    fn send_to<'a>(
        &'a self,
        bytes: &'a [u8],
        peer: SocketAddress,
    ) -> Pin<Box<dyn Future<Output = Result<usize, Self::Error>> + Send + 'a>>;
}
