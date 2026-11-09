//! Allocation-only received-payload copies and one inline Root notification.
use super::*;
impl crate::packet_output::PacketSender {
    pub(crate) fn has_received_memory(&self) -> bool {
        matches!(
            self.memory.as_ref().map(|binding| &binding.origin),
            Some(Origin::Original(_) | Origin::PendingOriginal(_))
        )
    }
    pub(crate) fn has_transmit_allocator(&self) -> bool {
        matches!(
            self.memory.as_ref().map(|binding| &binding.origin),
            Some(Origin::Original(_) | Origin::PendingOriginal(_) | Origin::Standalone(_))
        )
    }
    pub(crate) fn retain_received_payload(
        &self,
        packet: PacketBuffer,
        range: std::ops::Range<usize>,
    ) -> Result<PacketBuffer, (PacketDeriveError, PacketBuffer)> {
        let Some(view) = self.packet_memory_view() else {
            return Err((PacketDeriveError::Unreserved, packet));
        };
        view.retain_received_range(packet, range)
            .map_err(|(cause, packet)| (memory_error(cause), packet))
    }
    pub fn copy_received_payload(&self, bytes: &[u8]) -> Result<PacketBuffer, PacketDeriveError> {
        let view = self
            .packet_memory_view()
            .ok_or(PacketDeriveError::Unreserved)?;
        view.copy_received_packet(bytes).map_err(memory_error)
    }
    pub(crate) fn transmit_notification(
        &self,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let origin = self.memory.as_ref().map(|binding| binding.origin.clone());
        async move {
            match origin {
                Some(Origin::Original(view)) => view.changed().await,
                Some(Origin::PendingOriginal(cell)) => match cell.get() {
                    Some(view) => view.changed().await,
                    None => std::future::pending().await,
                },
                Some(Origin::Standalone(root)) => root.notified().await,
                Some(Origin::ExistingStorage) | None => std::future::pending().await,
            }
        }
    }
}
