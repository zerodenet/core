//! Opaque observations of the existing protocol payload queue, never timer/IO proof.
use super::*;
impl PeerTunnel {
    pub fn has_pending_packets(&self) -> bool {
        self.engine.queued_packet_count() != 0
    }
}
