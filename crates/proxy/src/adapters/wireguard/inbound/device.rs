use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};
use wireguard::runtime::InboundDevice;
pub(in crate::adapters::wireguard) struct LiveInboundDevice {
    pub(super) device: Mutex<InboundDevice>,
    pub(super) generation: AtomicU64,
    incarnation: AtomicU64,
    identity: Mutex<[u8; 32]>,
}

impl LiveInboundDevice {
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
