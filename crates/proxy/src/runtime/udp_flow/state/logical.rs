use super::{UdpFlowStartContext, UdpFlowState};
use crate::runtime::udp_flow::{
    outbound::UdpFlowOutbound,
    registered::LogicalConnection,
    result::{FlowFailure, FlowStartResult},
};
use zero_core::Session;

impl UdpFlowStartContext<'_> {
    pub(crate) async fn start_logical(
        &mut self,
        tag: String,
        session: &Session,
        payload: &[u8],
        connection: LogicalConnection,
    ) -> Result<FlowStartResult, FlowFailure> {
        let sent = connection.send(session, payload).await?;
        let managed = self
            .state
            .registered
            .register_logical(session.id, connection);
        Ok(FlowStartResult::Flow {
            outbound: Box::new(UdpFlowOutbound::LogicalStreamPacket { tag, managed }),
            tx_bytes: sent as u64,
        })
    }
}
impl UdpFlowState {
    pub(crate) fn release_logical_session(&mut self, session_id: u64) {
        self.registered.release_logical_session(session_id);
    }
}
