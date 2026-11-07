//! One original in-memory UDP registration; no additional task or native owner.
use super::*;
use std::sync::atomic::AtomicUsize;
use tokio::sync::Notify;

pub(super) struct SocketState {
    closed: AtomicBool,
    active: AtomicUsize,
    changed: Notify,
}
impl SocketState {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            closed: AtomicBool::new(false),
            active: AtomicUsize::new(0),
            changed: Notify::new(),
        })
    }
    pub(super) fn closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
    pub(super) fn stop(&self) {
        self.closed.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }
    async fn stopped(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.closed.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }
    fn enter(self: &Arc<Self>) -> Result<Active, ClientUdpSendError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ClientUdpSendError::closed(0));
        }
        self.active
            .try_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_add(1)
            })
            .map_err(|_| ClientUdpSendError::before(ClientUdpStackError::SocketLimit))?;
        let active = Active(self.clone());
        if self.closed.load(Ordering::Acquire) {
            return Err(ClientUdpSendError::closed(0));
        }
        Ok(active)
    }
}
struct Active(Arc<SocketState>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}
/// Exact logical removal/receive-drain observation. This says nothing about
/// already queued output, a caller's task join, or a physical tunnel/native IO.
#[derive(Clone)]
pub struct ClientUdpCloseReceipt {
    state: Arc<SocketState>,
    endpoint: Endpoint,
}
impl ClientUdpCloseReceipt {
    pub(super) fn new(state: Arc<SocketState>, endpoint: Endpoint) -> Self {
        Self { state, endpoint }
    }
    pub fn endpoint(&self) -> Endpoint {
        self.endpoint
    }
    pub fn same_original(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }
    pub fn io_quiescent(&self) -> bool {
        self.state.closed.load(Ordering::Acquire) && self.state.active.load(Ordering::Acquire) == 0
    }
    pub async fn wait_io_quiescent(&self) {
        loop {
            let changed = self.state.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.io_quiescent() {
                return;
            }
            changed.await;
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientUdpSendError {
    pub kind: ClientUdpStackError,
    /// Actual fragments already accepted by the original output queue. A
    /// nonzero count forbids retrying this datagram through another executor.
    pub accepted_fragments: usize,
}
impl std::fmt::Display for ClientUdpSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UDP stack {:?} after {} accepted fragments",
            self.kind, self.accepted_fragments
        )
    }
}
impl std::error::Error for ClientUdpSendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            ClientUdpStackError::PacketMemory(source) => Some(source),
            _ => None,
        }
    }
}
impl ClientUdpSendError {
    fn closed(accepted_fragments: usize) -> Self {
        Self {
            kind: ClientUdpStackError::OutboundClosed,
            accepted_fragments,
        }
    }
    fn before(kind: ClientUdpStackError) -> Self {
        Self {
            kind,
            accepted_fragments: 0,
        }
    }
}
#[derive(Clone)]
pub struct ClientUdpSender {
    inner: Arc<Inner>,
    endpoint: Endpoint,
    observer: Option<Arc<dyn zero_traits::IoObserver>>,
    state: Arc<SocketState>,
}
impl ClientUdpSender {
    pub(super) fn new(
        inner: Arc<Inner>,
        endpoint: Endpoint,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
        state: Arc<SocketState>,
    ) -> Self {
        Self {
            inner,
            endpoint,
            observer,
            state,
        }
    }
    #[cfg(feature = "packet-memory")]
    pub fn copy_pending_payload(
        &self,
        bytes: &[u8],
    ) -> Result<zero_traits::PacketBuffer, zero_traits::PacketDeriveError> {
        self.inner.outbound.copy_pending_payload(bytes)
    }
    #[cfg(feature = "packet-memory")]
    pub fn defer_memory_notifications(
        &self,
    ) -> Option<crate::packet_output::memory::PacketSendNotifications> {
        self.inner.outbound.defer_memory_notifications()
    }
    pub fn local_endpoint(&self) -> Endpoint {
        self.endpoint
    }
    pub async fn send_to(
        &self,
        payload: &[u8],
        destination: Endpoint,
    ) -> Result<(), ClientUdpSendError> {
        let _active = self.state.enter()?;
        if self.endpoint.ip.is_ipv4() != destination.ip.is_ipv4() {
            return Err(ClientUdpSendError::before(
                ClientUdpStackError::AddressFamilyMismatch,
            ));
        }
        let maximum = if destination.ip.is_ipv4() {
            65_507
        } else {
            65_527
        };
        if payload.len() > maximum {
            return Err(ClientUdpSendError::before(
                ClientUdpStackError::PayloadTooLarge,
            ));
        }
        let mtu = {
            let mut entries = self.inner.sockets.lock().unwrap_or_else(|e| e.into_inner());
            let entry = entries
                .get_mut(&self.endpoint)
                .filter(|entry| Arc::ptr_eq(&entry.state, &self.state))
                .ok_or_else(|| ClientUdpSendError::closed(0))?;
            if entry.last_destination != Some(destination) {
                entry.path_mtu = None;
                entry.last_destination = Some(destination);
            }
            entry.path_mtu.unwrap_or(self.inner.mtu)
        };
        let identification = self.inner.next_fragment_id.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "packet-memory")]
        let owned = if self.inner.outbound.has_packet_memory() {
            Some(
                super::transmit::build(
                    &self.inner.outbound,
                    packet::UdpPacketSpec {
                        source: self.endpoint.ip,
                        destination: destination.ip,
                        source_port: self.endpoint.port,
                        destination_port: destination.port,
                        payload,
                    },
                    mtu,
                    identification,
                )
                .await
                .map_err(|source| {
                    ClientUdpSendError::before(ClientUdpStackError::PacketMemory(source))
                })?,
            )
        } else {
            None
        };
        #[cfg(not(feature = "packet-memory"))]
        let owned: Option<Vec<zero_traits::PacketBuffer>> = None;
        let fragments = match owned {
            Some(fragments) => fragments,
            None => {
                // Explicit library senders have no Host memory binding. Every
                // production Source/Device installs its actual Kernel binding.
                let packet = packet::build_udp(
                    self.endpoint.ip,
                    destination.ip,
                    self.endpoint.port,
                    destination.port,
                    payload,
                );
                packet::fragment_ip_packet_owned(packet, mtu, identification)
                    .into_iter()
                    .map(Into::into)
                    .collect()
            }
        };
        if fragments.is_empty() {
            return Err(ClientUdpSendError::before(ClientUdpStackError::InvalidMtu));
        }
        let mut accepted = 0;
        for fragment in fragments {
            if self.inner.closed.load(Ordering::Acquire)
                || self.state.closed.load(Ordering::Acquire)
            {
                return Err(ClientUdpSendError::closed(accepted));
            }
            let output = self
                .inner
                .outbound
                .clone()
                .with_observer(self.observer.clone());
            let result = tokio::select! { biased;
                _ = self.state.stopped() => {
                    observe_discard(self.observer.as_ref(), self.inner.drop_observer.as_ref(), zero_traits::PacketDropReason::QueueClosed);
                    return Err(ClientUdpSendError::closed(accepted));
                },
                result = output.send_buffer(fragment) => result,
            };
            if result.is_err() {
                observe_discard(
                    self.observer.as_ref(),
                    self.inner.drop_observer.as_ref(),
                    zero_traits::PacketDropReason::QueueClosed,
                );
                return Err(ClientUdpSendError::closed(accepted));
            }
            accepted += 1;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/client_udp/sender.rs"]
mod tests;

#[cfg(all(test, feature = "packet-memory"))]
#[path = "../../tests/client_udp/pending_memory.rs"]
mod pending_memory_tests;
