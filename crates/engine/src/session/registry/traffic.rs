//! Protocol-supplied peer bindings use the same per-session direction claims.
use super::{ActiveSessionEntry, SessionRegistry};
use crate::TrafficMeter;

impl SessionRegistry {
    pub(crate) fn bind_peer_traffic(&self, id: u64, outbound: bool, meter: TrafficMeter) {
        if let Some(entry) = self.get(id) {
            meter.observes_role(zero_api::TrafficPlane::Flow, outbound);
            let mut peers = entry
                .peer_traffic
                .write()
                .unwrap_or_else(|e| e.into_inner());
            peers[outbound as usize] = Some(
                peers
                    .iter()
                    .flatten()
                    .find(|old| old.meter.same_source(&meter))
                    .cloned()
                    .unwrap_or_else(|| {
                        crate::session::accounting::EndpointFlowMeter::peer(meter, entry.network)
                    }),
            );
            entry
                .peer_traffic_available
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }
}
impl ActiveSessionEntry {
    pub(super) fn record_peer_boundary(&self, outbound: bool, rx: bool, bytes: u64) {
        if !self
            .peer_traffic_available
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return;
        }
        if let Some(peer) =
            &self.peer_traffic.read().unwrap_or_else(|e| e.into_inner())[outbound as usize]
        {
            peer.record(outbound, rx, bytes);
        }
    }
}
