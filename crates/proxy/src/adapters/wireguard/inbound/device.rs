use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};
use wireguard::runtime::InboundDevice;
pub(in crate::adapters::wireguard) struct LiveInboundDevice {
    pub(super) device: Mutex<InboundDevice>,
    pub(super) generation: AtomicU64,
    incarnation: AtomicU64,
    identity: Mutex<[u8; 32]>,
    listener_active: AtomicBool,
}

impl LiveInboundDevice {
    pub(in crate::adapters::wireguard) fn listener_active(&self) -> bool {
        self.listener_active.load(Ordering::Acquire)
    }

    pub(in crate::adapters::wireguard) fn claim_listener(
        self: &Arc<Self>,
    ) -> Result<ListenerLease, zero_engine::EngineError> {
        self.listener_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| super::invalid("WireGuard crypto owner already has a listener"))?;
        Ok(ListenerLease(self.clone()))
    }

    pub(in crate::adapters::wireguard) fn incarnation(&self) -> u64 {
        self.incarnation.load(Ordering::Acquire)
    }
    pub(in crate::adapters::wireguard) fn peer_source(
        &self,
        peer_index: usize,
    ) -> Option<wireguard::runtime::PeerSourceObservation> {
        self.device
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .peer_source(peer_index)
    }

    pub(in crate::adapters::wireguard) fn new(device: InboundDevice, identity: [u8; 32]) -> Self {
        Self {
            device: Mutex::new(device),
            generation: AtomicU64::new(0),
            incarnation: AtomicU64::new(crate::runtime::raw_ip::next_incarnation()),
            identity: Mutex::new(identity),
            listener_active: AtomicBool::new(false),
        }
    }

    pub(in crate::adapters::wireguard) fn matches_identity(&self, identity: [u8; 32]) -> bool {
        *self
            .identity
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            == identity
    }

    pub(in crate::adapters::wireguard) fn replace(
        &self,
        device: InboundDevice,
        identity: [u8; 32],
    ) {
        let mut current = self
            .device
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !current.replace_preserving_peers(device) {
            self.generation.fetch_add(1, Ordering::Release);
            self.incarnation.store(
                crate::runtime::raw_ip::next_incarnation(),
                Ordering::Release,
            );
        }
        *self
            .identity
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = identity;
    }
}

// This lease describes actual listener selection only. Dropping it proves no
// task join, native close, transferred-user closure, or permission to replace.
pub(in crate::adapters::wireguard) struct ListenerLease(Arc<LiveInboundDevice>);
impl Drop for ListenerLease {
    fn drop(&mut self) {
        self.0.listener_active.store(false, Ordering::Release);
    }
}
