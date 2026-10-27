use crate::runtime::udp_dispatch::UdpDispatch;
#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "managed-datagram-runtime"
))]
use crate::runtime::udp_flow::managed::ManagedUdpFlowRequest;
use crate::runtime::udp_flow::managed::ManagedUdpFlowResume;
use crate::runtime::udp_flow::outbound::ManagedUdpFlowRef;
use crate::runtime::udp_flow::state::UdpFlowStartContext;

impl UdpDispatch {
    pub(crate) fn flow_start_context(&mut self) -> UdpFlowStartContext<'_> {
        UdpFlowStartContext::new(&mut self.flow_state)
    }

    #[cfg(any(
        feature = "managed-stream-runtime",
        feature = "managed-datagram-runtime"
    ))]
    pub(crate) async fn start_managed_flow(
        &mut self,
        request: ManagedUdpFlowRequest<'_>,
    ) -> Result<usize, crate::runtime::udp_dispatch::FlowFailure> {
        self.flow_state.start_managed_flow(request).await
    }

    #[cfg(any(
        feature = "upstream-association-runtime",
        feature = "managed-datagram-runtime"
    ))]
    pub(crate) fn register_managed_flow(
        &mut self,
        resume: ManagedUdpFlowResume,
    ) -> ManagedUdpFlowRef {
        self.flow_state.register_managed_flow(resume)
    }
}
