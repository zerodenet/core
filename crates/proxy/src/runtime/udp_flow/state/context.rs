#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "managed-datagram-runtime"
))]
use crate::runtime::udp_flow::managed::ManagedUdpFlowRequest;
use crate::runtime::udp_flow::managed::ManagedUdpFlowResume;
use crate::runtime::udp_flow::outbound::ManagedUdpFlowRef;
use crate::runtime::udp_flow::result::FlowFailure;

use super::UdpFlowState;

pub(crate) struct UdpFlowStartContext<'a> {
    inbound_tag: &'a str,
    pub(super) state: &'a mut UdpFlowState,
}

impl<'a> UdpFlowStartContext<'a> {
    pub(crate) fn new(inbound_tag: &'a str, state: &'a mut UdpFlowState) -> Self {
        Self { inbound_tag, state }
    }

    #[cfg(any(
        feature = "managed-stream-runtime",
        feature = "managed-datagram-runtime"
    ))]
    pub(crate) async fn start_managed_flow(
        &mut self,
        request: ManagedUdpFlowRequest<'_>,
    ) -> Result<usize, FlowFailure> {
        self.state
            .start_managed_flow(self.inbound_tag, request)
            .await
    }

    pub(crate) fn register_managed_flow(
        &mut self,
        resume: ManagedUdpFlowResume,
    ) -> ManagedUdpFlowRef {
        self.state.register_managed_flow(resume)
    }
}
