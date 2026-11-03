use crate::protocol_registry::UdpRuntimeServices;
use crate::runtime::udp_flow::outbound::ManagedUdpFlowRef;
#[cfg(feature = "upstream-association-runtime")]
use crate::runtime::udp_flow::registered::UpstreamAssociationSend;
use crate::runtime::udp_flow::result::FlowFailure;

use super::UdpFlowState;

impl UdpFlowState {
    pub(crate) fn release_session(&mut self, session_id: u64) {
        self.registered.release_session(session_id);
    }

    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) async fn start_upstream_flow(
        &mut self,
        inbound_tag: &str,
        request: UpstreamAssociationSend<'_>,
    ) -> Result<usize, FlowFailure> {
        self.registered
            .start_upstream_udp_flow(inbound_tag, request)
            .await
    }

    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn upstream_flow_target(
        &self,
        id: ManagedUdpFlowRef,
    ) -> Option<super::super::registered::DatagramAssociationTarget> {
        self.registered.upstream_flow_target(id)
    }
    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn register_upstream_flow(
        &mut self,
        target: super::super::registered::DatagramAssociationTarget,
    ) -> ManagedUdpFlowRef {
        self.registered.register_upstream_flow(target)
    }

    #[cfg(any(
        feature = "managed-stream-runtime",
        feature = "managed-datagram-runtime"
    ))]
    pub(crate) async fn forward_existing_managed_flow(
        &mut self,
        services: UdpRuntimeServices,
        request: (&super::super::snapshot::UdpFlowSnapshot, &[u8]),
    ) -> Result<usize, FlowFailure> {
        self.registered
            .forward_existing_managed_flow(&mut self.chain_tasks, services, request)
            .await
    }
}
