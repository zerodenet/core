//! Opaque observations of the existing protocol payload queue, never timer/IO proof.
use super::*;
impl PeerTunnel {
    pub fn has_pending_packets(&self) -> bool {
        self.engine.queued_packet_count() != 0
            || self
                .prepared_data
                .as_ref()
                .is_some_and(|pending| pending.live())
    }
}
