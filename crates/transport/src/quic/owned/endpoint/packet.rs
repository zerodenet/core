//! Bind only the exact endpoint socket and its same unique packet queues.
use super::*;
impl OwnedEndpointOwner {
    pub fn bind_packet_segment(
        &self,
        segment: &super::super::packet::PacketSegment,
    ) -> Result<(), ChannelError> {
        let Native::Packet(socket) = &self.native else {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        };
        if self.shared.phase.load(Ordering::Acquire) != 0 {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        segment.bind_endpoint(socket, self.drivers.registration_ledger())
    }
}
