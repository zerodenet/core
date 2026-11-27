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
    socket: Option<ControlledDatagramSocket>,
    returned: BoundDatagramView,
    binding: DatagramBindingId,
    selection: EgressSelection,
    authorization: Arc<dyn DatagramBindingAuthorization>,
}
impl fmt::Debug for BoundDatagramSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundDatagramSocket")
            .field("binding", &self.binding)
            .field("reservation", &self.socket().reservation())
            .finish_non_exhaustive()
    }
}
fn stale(stage: Stage) -> ChannelError {
    let mut failure = ChannelError::new(stage, Reason::StalePlan, Impact::Session);
    failure.delivery = DeliveryState::NotSubmitted;
    failure
}
fn unsubmitted_send(source: ChannelError) -> ChannelError {
    let mut failure = ChannelError::new(Stage::Send, source.reason, Impact::Session);
    failure.component = source.component;
    failure.session = source.session;
    failure.delivery = DeliveryState::NotSubmitted;
    failure.with_source(source)
}
#[derive(Debug)]
struct DatagramSelectionMismatch(String);
impl fmt::Display for DatagramSelectionMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for DatagramSelectionMismatch {}
fn format_interface(interface: Option<&EgressInterface>) -> String {
    match interface {
        Some(iface) => format!("{}#{}", iface.name(), iface.index()),
        None => "none".to_owned(),
    }
}
/// Observation of the original unique IO view's actual return. Its flag is
/// set after dropping that IO view, independently of native close/Scope joins.
#[derive(Clone)]
pub struct BoundDatagramView {
    original: crate::controlled_datagram::ControlledDatagramIdentity,
    returned: Arc<std::sync::atomic::AtomicBool>,
}
impl BoundDatagramView {
    pub fn is_returned(&self) -> bool {
        self.returned.load(std::sync::atomic::Ordering::Acquire)
    }
    pub fn matches_control(
        &self,
        control: &crate::controlled_datagram::ControlledDatagramControl,
    ) -> bool {
        self.original.matches_control(control)
    }
}
impl BoundDatagramSocket {
    fn socket(&self) -> &ControlledDatagramSocket {
        self.socket.as_ref().expect("live unique original UDP view")
    }
    fn socket_mut(&mut self) -> &mut ControlledDatagramSocket {
        self.socket.as_mut().expect("live unique original UDP view")
    }
    pub fn return_observation(&self) -> BoundDatagramView {
        self.returned.clone()
    }

    /// Read the sealed registration of this same acquired original. The binding
    /// ID or a peer address cannot manufacture a provider or native-close proof.
    pub fn original(&self) -> Option<crate::controlled_datagram::provider::NativeDatagramOrigin> {
        self.socket().original_provider()?.origin()
    }

    pub(super) fn new(
        socket: ControlledDatagramSocket,
        binding: DatagramBindingId,
        selection: EgressSelection,
        authorization: Arc<dyn DatagramBindingAuthorization>,
    ) -> Self {
        let returned = BoundDatagramView {
            original: socket.identity(),
            returned: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        Self {
            socket: Some(socket),
            returned,
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
        self.socket().reservation()
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket().local_addr()
    }
    pub fn egress_interface(&self) -> Option<&EgressInterface> {
        self.selection.interface()
    }
    pub fn set_observer(
        &mut self,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> io::Result<()> {
        self.socket_mut().set_observer(observer)
    }
    fn validate_send(&self, peer: SocketAddr) -> Result<(), ChannelError> {
        let generation = NetworkGeneration(self.selection.generation());
        self.authorization
            .check_current(generation)
            .map_err(unsubmitted_send)?;
        let selected = self
            .authorization
            .select(peer, generation)
            .map_err(unsubmitted_send)?;
        let local_addr = self
            .socket()
            .local_addr()
            .map_err(|source| stale(Stage::Send).with_source(source))?;
        let normalized_peer = self.authorization.policy().normalize_peer(peer).ok();
        let generation_changed = selected.generation() != self.selection.generation();
        let source_changed = selected.dial_source_address() != self.selection.dial_source_address();
        let interface_changed = selected.interface() != self.selection.interface();
        let family_changed = peer.is_ipv6() != local_addr.is_ipv6();
        let peer_rejected = normalized_peer != Some(peer);
        if generation_changed
            || source_changed
            || interface_changed
            || family_changed
            || peer_rejected
        {
            let mut invalidated = false;
            if !generation_changed && (source_changed || interface_changed) {
                invalidated = self.authorization.note_stale_selection(generation);
            }
            let mut reasons = Vec::new();
            if generation_changed {
                reasons.push(format!(
                    "generation {} -> {}",
                    self.selection.generation(),
                    selected.generation()
                ));
            }
            if source_changed {
                reasons.push(format!(
                    "dial_source {:?} -> {:?} (route_source={:?}, reason={})",
                    self.selection.dial_source_address(),
                    selected.dial_source_address(),
                    selected.route_source(),
                    selected.binding_reason().as_str()
                ));
            }
            if interface_changed {
                reasons.push(format!(
                    "interface {} -> {} (reason={})",
                    format_interface(self.selection.interface()),
                    format_interface(selected.interface()),
                    selected.binding_reason().as_str()
                ));
            }
            if family_changed {
                reasons.push(format!(
                    "family local_ipv6={} peer_ipv6={}",
                    local_addr.is_ipv6(),
                    peer.is_ipv6()
                ));
            }
            if peer_rejected {
                reasons.push(format!("peer_policy normalized={normalized_peer:?}"));
            }
            let detail = format!(
                "bound UDP egress selection mismatch for peer {peer} on local {local_addr} (invalidated={invalidated}): {}",
                reasons.join("; ")
            );
            return Err(stale(Stage::Send).with_source(DatagramSelectionMismatch(detail)));
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
        self.socket().poll_send_to_addr(cx, bytes, peer)
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
        self.socket().poll_recv_from_addr(cx, bytes)
    }
    pub async fn recv_from_addr(&self, bytes: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        poll_fn(|cx| self.poll_recv_from_addr(cx, bytes)).await
    }
    /// Physical evidence only; the provider alone consumes its owner's receipt.
    pub fn close(&self) -> Result<DatagramCloseReceipt, Arc<io::Error>> {
        self.socket().close()
    }
    /// Same physical original. A concurrent native operation remains pending,
    /// not an Unknown failure or a returned provider receipt.
    pub fn try_close(&self) -> Option<Result<DatagramCloseReceipt, Arc<io::Error>>> {
        self.socket().try_close()
    }
}

impl Drop for BoundDatagramSocket {
    fn drop(&mut self) {
        drop(self.socket.take());
        self.returned
            .returned
            .store(true, std::sync::atomic::Ordering::Release);
    }
}
