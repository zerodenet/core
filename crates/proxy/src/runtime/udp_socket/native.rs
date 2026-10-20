//! Original native I/O; controlled mode keeps its FD in the same state owner.
use std::{
    io,
    net::SocketAddr,
    sync::Arc,
    task::{Context, Poll},
};
use zero_platform_tokio::{
    controlled_datagram::ControlledDatagramSocket, EgressInterface, TokioDatagramSocket,
};
pub(super) enum DirectDatagramIo {
    Raw(TokioDatagramSocket),
    Controlled(ControlledDatagramSocket),
}
impl DirectDatagramIo {
    pub(super) fn local_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Self::Raw(socket) => socket.local_addr(),
            Self::Controlled(socket) => socket.local_addr(),
        }
    }
    pub(super) fn egress_interface(&self) -> Option<&EgressInterface> {
        match self {
            Self::Raw(socket) => socket.egress_interface(),
            Self::Controlled(socket) => socket.egress_interface(),
        }
    }
    pub(super) fn set_observer(
        &mut self,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> io::Result<()> {
        match self {
            Self::Raw(socket) => {
                socket.set_observer(observer);
                Ok(())
            }
            Self::Controlled(socket) => socket.set_observer(observer),
        }
    }
    pub(super) async fn send_to_addr(&self, bytes: &[u8], peer: SocketAddr) -> io::Result<usize> {
        match self {
            Self::Raw(socket) => socket.send_to_addr(bytes, peer).await,
            Self::Controlled(socket) => socket.send_to_addr(bytes, peer).await,
        }
    }
    pub(super) fn poll_recv_from_addr(
        &self,
        cx: &mut Context<'_>,
        bytes: &mut [u8],
    ) -> Poll<io::Result<(usize, SocketAddr)>> {
        match self {
            Self::Raw(socket) => socket.poll_recv_from_addr(cx, bytes),
            Self::Controlled(socket) => socket.poll_recv_from_addr(cx, bytes),
        }
    }
}
