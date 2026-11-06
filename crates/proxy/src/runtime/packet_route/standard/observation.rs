//! A single actual owned submission's selection receipt, never a peer cache.
use std::sync::{Arc, Mutex};
use zero_core::{
    component::{PeerId, SessionId},
    packet::ReturnAssociation,
};
use zero_platform_tokio::contract::context::{ConnectFact, ConnectObserver, PacketIoFact};
pub(super) struct Observation {
    inner: Option<Arc<dyn zero_traits::IoObserver>>,
    submitting: Mutex<Option<(ReturnAssociation, Option<PeerId>)>>,
}
impl Observation {
    pub(super) fn new(inner: Option<Arc<dyn zero_traits::IoObserver>>) -> Self {
        Self {
            inner,
            submitting: Mutex::new(None),
        }
    }
    pub(super) fn discard(&self, packet: &zero_core::packet::IpPacket) {
        self.packet_io(
            &packet.return_association,
            PacketIoFact::Dropped {
                bytes: packet.payload().len(),
                reason: zero_traits::PacketDropReason::PolicyRejected,
            },
        );
    }
    pub(super) fn begin(&self, association: &ReturnAssociation) {
        *self.submitting.lock().unwrap() = Some((association.clone(), None));
    }
    pub(super) fn finish(&self, association: &ReturnAssociation) -> Option<Arc<str>> {
        self.submitting
            .lock()
            .unwrap()
            .take()
            .filter(|(original, _)| original == association)
            .and_then(|(_, peer)| peer)
            .map(|peer| peer.0)
    }
}
impl ConnectObserver for Observation {
    fn record(&self, _: Option<SessionId>, _: ConnectFact) {}
    fn packet_flow_io(&self) -> Option<Arc<dyn zero_traits::IoObserver>> {
        self.inner.clone()
    }
    fn packet_peer_selected_for(&self, association: &ReturnAssociation, peer: PeerId) {
        if let Some((original, selected)) = &mut *self.submitting.lock().unwrap() {
            if original == association {
                *selected = Some(peer);
            }
        }
    }
    fn packet_io(&self, _: &ReturnAssociation, fact: PacketIoFact) {
        let Some(observer) = &self.inner else {
            return;
        };
        match fact {
            PacketIoFact::Sent { bytes } => observer.sent(bytes),
            PacketIoFact::Received { bytes } => observer.received(bytes),
            PacketIoFact::Dropped { reason, .. } => observer.dropped_reason(reason),
            PacketIoFact::Error => observer.error(),
            PacketIoFact::ReceiveCoverageLost => observer.receive_coverage_lost(),
        }
    }
}
