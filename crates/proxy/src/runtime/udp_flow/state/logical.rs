use super::UdpFlowStartContext;
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
        upstream: Option<(String, u16)>,
        session: &Session,
        payload: &[u8],
        connection: LogicalConnection,
    ) -> Result<FlowStartResult, FlowFailure> {
        let sent = connection
            .send(session, payload)
            .await
            .map_err(|mut failure| {
                failure.upstream = upstream.clone();
                failure
            })?;
        let remote = connection.observed_remote();
        let managed = self
            .state
            .registered
            .register_logical(session.id, connection);
        let outbound = match upstream {
            Some((server, port)) => UdpFlowOutbound::StreamPacket {
                tag,
                server,
                port,
                managed,
                remote,
            },
            None => UdpFlowOutbound::LogicalStreamPacket { tag, managed },
        };
        Ok(FlowStartResult::Flow {
            outbound: Box::new(outbound),
            tx_bytes: sent as u64,
        })
    }
}
