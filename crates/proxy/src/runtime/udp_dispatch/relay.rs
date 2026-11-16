use crate::protocol_registry::UdpAdapterContext;
use crate::runtime::udp_dispatch::{FlowFailure, FlowStartResult, UdpDispatch};
use crate::runtime::udp_flow::packet_path::PacketPathFlowBinding;
use crate::runtime::udp_flow::packet_path_chain::PacketPathStartRequest;

pub(crate) struct PreparedUdpRelayChain<'a> {
    pub(crate) flow_binding: PacketPathFlowBinding,
    pub(crate) request: Box<PacketPathStartRequest<'a>>,
}

impl PreparedUdpRelayChain<'_> {
    pub(crate) async fn execute(
        self,
        dispatch: &mut UdpDispatch,
        ctx: UdpAdapterContext<'_>,
        _session: &zero_core::Session,
        _payload: &[u8],
    ) -> Result<FlowStartResult, FlowFailure> {
        let sent = dispatch.send_packet_path_chain(ctx, *self.request).await?;
        Ok(FlowStartResult::Flow {
            outbound: Box::new(UdpDispatch::datagram_chain_flow_outbound(self.flow_binding)),
            tx_bytes: sent as u64,
        })
    }
}
