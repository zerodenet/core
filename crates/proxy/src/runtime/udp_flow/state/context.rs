use super::UdpFlowState;

pub(crate) struct UdpFlowStartContext<'a> {
    pub(in crate::runtime::udp_flow) state: &'a mut UdpFlowState,
}

impl<'a> UdpFlowStartContext<'a> {
    pub(crate) fn new(state: &'a mut UdpFlowState) -> Self {
        Self { state }
    }
}
