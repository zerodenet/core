//! Native packet devices cannot enforce socket constraints, including at execution.
use super::HostPacketDevice;
use crate::runtime::packet_route::{PacketForwardObservation, PreparedPacketRouteOperation};
use std::{io, sync::Arc};
pub(crate) struct PreparedHostPacketOperation {
    pub(crate) device: Arc<HostPacketDevice>,
    pub(crate) dial_policy: zero_traits::DialPolicy,
}
#[async_trait::async_trait]
impl PreparedPacketRouteOperation for PreparedHostPacketOperation {
    async fn forward(
        &self,
        packet: &mut zero_traits::PacketBuffer,
        ingress_id: u64,
        replies: zero_stack::packet_output::PacketSender,
        egress_generation: u64,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> io::Result<PacketForwardObservation> {
        if self.dial_policy != zero_traits::DialPolicy::default() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "native PacketSink cannot enforce outbound dial policy",
            ));
        }
        self.device
            .forward(packet, ingress_id, replies, egress_generation, observer)
            .await
    }
}
