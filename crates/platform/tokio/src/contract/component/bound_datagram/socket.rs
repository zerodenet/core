use super::super::super::{DeliveryState, Impact, Reason, Stage};
use super::*;
use crate::{
    controlled_datagram::{DatagramCloseReceipt, DatagramReservationId},
    EgressInterface,
};
use std::{
    future::poll_fn,
    io,
    task::{Context, Poll},
};
/// A narrow current-source fence. It neither owns the provider nor exposes an opener.
#[derive(Clone)]
pub struct DatagramBindingGuard {
    authorization: Arc<dyn DatagramBindingAuthorization>,
    generation: NetworkGeneration,
}
impl DatagramBindingGuard {
    pub fn check_current(&self) -> Result<(), ChannelError> {
        self.authorization.check_current(self.generation)
    }
}
/// A unique I/O view. Neither the raw socket nor a global opener can be recovered.
pub struct BoundDatagramSocket {
    socket: ControlledDatagramSocket,
    binding: DatagramBindingId,
    selection: EgressSelection,
    authorization: Arc<dyn DatagramBindingAuthorization>,
}
impl fmt::Debug for BoundDatagramSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundDatagramSocket")
            .field("binding", &self.binding)
            .field("reservation", &self.socket.reservation())
            .finish_non_exhaustive()
    }
}
fn stale(stage: Stage) -> ChannelError {
    let mut failure = ChannelError::new(stage, Reason::StalePlan, Impact::Session);
    failure.delivery = DeliveryState::NotSubmitted;
    failure
}
impl BoundDatagramSocket {
    pub(super) fn new(
        socket: ControlledDatagramSocket,
        binding: DatagramBindingId,
        selection: EgressSelection,
        authorization: Arc<dyn DatagramBindingAuthorization>,
    ) -> Self {
        Self {
            socket,
            binding,
            selection,
            authorization,
        }
    }
    pub fn guard(&self) -> DatagramBindingGuard {
        DatagramBindingGuard {
            authorization: self.authorization.clone(),
            generation: NetworkGeneration(self.selection.generation()),
        }
    }
    pub fn binding(&self) -> &DatagramBindingId {
        &self.binding
    }
    pub fn reservation(&self) -> DatagramReservationId {
        self.socket.reservation()
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
    pub fn egress_interface(&self) -> Option<&EgressInterface> {
        self.selection.interface()
    }
    pub fn set_observer(
        &mut self,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> io::Result<()> {
        self.socket.set_observer(observer)
    }
    fn validate_send(&self, peer: SocketAddr) -> Result<(), ChannelError> {
        let generation = NetworkGeneration(self.selection.generation());
        self.authorization.check_current(generation)?;
        let selected = self.authorization.select(peer, generation)?;
        if selected.generation() != self.selection.generation()
            || selected.dial_source_address() != self.selection.dial_source_address()
            || selected.interface() != self.selection.interface()
            || peer.is_ipv6()
                != self
                    .socket
                    .local_addr()
                    .map_err(|source| stale(Stage::Send).with_source(source))?
                    .is_ipv6()
            || self.authorization.policy().normalize_peer(peer).ok() != Some(peer)
        {
            return Err(stale(Stage::Send));
        }
        Ok(())
    }
    pub fn poll_send_to_addr(
        &self,
        cx: &mut Context<'_>,
        bytes: &[u8],
        peer: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        if let Err(source) = self.validate_send(peer) {
            return Poll::Ready(Err(io::Error::other(source)));
        }
        self.socket.poll_send_to_addr(cx, bytes, peer)
    }
    pub async fn send_to_addr(&self, bytes: &[u8], peer: SocketAddr) -> io::Result<usize> {
        poll_fn(|cx| self.poll_send_to_addr(cx, bytes, peer)).await
    }
    pub fn poll_recv_from_addr(
        &self,
        cx: &mut Context<'_>,
        bytes: &mut [u8],
    ) -> Poll<io::Result<(usize, SocketAddr)>> {
        if let Err(source) = self
            .authorization
            .check_current(NetworkGeneration(self.selection.generation()))
        {
            // A source change parks the old receive view. The sole driver
            // cancels this poll before its real rotation command closes the
            // original. Do not turn a healthy reload race into native I/O fault.
            if matches!(
                source.reason,
                Reason::StalePlan | Reason::Revoked | Reason::Cancelled
            ) {
                return Poll::Pending;
            }
            return Poll::Ready(Err(io::Error::other(source)));
        }
        self.socket.poll_recv_from_addr(cx, bytes)
    }
    pub async fn recv_from_addr(&self, bytes: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        poll_fn(|cx| self.poll_recv_from_addr(cx, bytes)).await
    }
    /// Physical evidence only; the provider alone consumes its owner's receipt.
    pub fn close(&self) -> Result<DatagramCloseReceipt, Arc<io::Error>> {
        self.socket.close()
    }
}
