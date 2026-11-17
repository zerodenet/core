use super::{state::RegisteredUdpConnection, RegisteredUdpState};
use crate::protocol_registry::UdpRuntimeServices;
use crate::runtime::udp_flow::{
    response_join::ResponseSetLease, result::FlowFailure, snapshot::UdpFlowSnapshot,
};
impl RegisteredUdpState {
    pub(crate) async fn forward_existing_managed_flow(
        &mut self,
        _tasks: &mut ResponseSetLease,
        _services: UdpRuntimeServices,
        request: (&UdpFlowSnapshot, &[u8]),
    ) -> Result<usize, FlowFailure> {
        let (flow, payload) = request;
        let id = flow.outbound.managed_flow().ok_or_else(|| {
            unavailable("protocol UDP flow has no executable connection reference")
        })?;
        let connection = self
            .connections
            .get(&id)
            .cloned()
            .ok_or_else(|| unavailable("UDP connection was dropped"))?;
        match connection {
            #[cfg(feature = "managed-stream-runtime")]
            RegisteredUdpConnection::Logical(connection) => {
                connection.send(&flow.session, payload).await
            }
            #[cfg(feature = "upstream-association-runtime")]
            RegisteredUdpConnection::Upstream(_) => Err(unavailable(
                "upstream association flows are handled by generic UDP dispatch",
            )),
        }
    }
}
fn unavailable(message: &'static str) -> FlowFailure {
    FlowFailure {
        stage: "udp_protocol_forward",
        error: zero_engine::EngineError::Io(std::io::Error::other(message)),
        upstream: None,
    }
}
