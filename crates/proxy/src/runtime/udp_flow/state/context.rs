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
    pub(super) state: &'a mut UdpFlowState,
}

impl<'a> UdpFlowStartContext<'a> {
    pub(crate) fn new(state: &'a mut UdpFlowState) -> Self {
        Self { state }
    }

    #[cfg(any(
        feature = "managed-stream-runtime",
        feature = "managed-datagram-runtime"
    ))]
    pub(crate) async fn start_managed_flow(
        &mut self,
        request: ManagedUdpFlowRequest<'_>,
    ) -> Result<usize, FlowFailure> {
        self.state.start_managed_flow(request).await
    }

    pub(crate) fn register_managed_flow(
        &mut self,
        resume: ManagedUdpFlowResume,
    ) -> ManagedUdpFlowRef {
        self.state.register_managed_flow(resume)
    }
}
