#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "managed-datagram-runtime"
))]
use crate::protocol_registry::UdpRuntimeServices;
#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "managed-datagram-runtime"
))]
use crate::runtime::udp_flow::managed::ManagedExistingFlowForward;
#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "managed-datagram-runtime"
))]
use crate::runtime::udp_flow::managed::ManagedUdpFlowRequest;
use crate::runtime::udp_flow::managed::ManagedUdpFlowResume;
use crate::runtime::udp_flow::outbound::ManagedUdpFlowRef;
#[cfg(feature = "upstream-association-runtime")]
use crate::runtime::udp_flow::registered::UpstreamAssociationSend;
use crate::runtime::udp_flow::result::FlowFailure;

use super::UdpFlowState;

impl UdpFlowState {
    #[cfg(any(
        feature = "managed-stream-runtime",
        feature = "managed-datagram-runtime"
    ))]
    pub(crate) async fn start_managed_flow(
        &mut self,
        mut request: ManagedUdpFlowRequest<'_>,
    ) -> Result<usize, FlowFailure> {
        request.chain_tasks = Some(&mut self.chain_tasks);
        self.registered.start_managed_udp_flow(request).await
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

    pub(crate) fn register_managed_flow(
        &mut self,
        resume: ManagedUdpFlowResume,
    ) -> ManagedUdpFlowRef {
        self.registered.register_managed_flow(resume)
    }

    #[cfg(any(
        feature = "managed-stream-runtime",
        feature = "managed-datagram-runtime"
    ))]
    pub(crate) async fn forward_existing_managed_flow(
        &mut self,
        services: UdpRuntimeServices,
        request: ManagedExistingFlowForward<'_>,
    ) -> Result<usize, FlowFailure> {
        self.registered
            .forward_existing_managed_flow(&mut self.chain_tasks, services, request)
            .await
    }
}
