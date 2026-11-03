use crate::runtime::udp_dispatch::UdpDispatch;
use crate::runtime::udp_flow::state::UdpFlowStartContext;
impl UdpDispatch {
    pub(crate) fn flow_start_context(&mut self) -> UdpFlowStartContext<'_> {
        UdpFlowStartContext::new(&mut self.flow_state)
    }
}
