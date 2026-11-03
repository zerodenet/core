use super::{state::RegisteredUdpConnection, RegisteredUdpState};
use crate::protocol_registry::UdpRuntimeServices;
use crate::runtime::udp_flow::{
    packet_path::ChainTask, result::FlowFailure, snapshot::UdpFlowSnapshot,
};
use tokio::task::JoinSet;
impl RegisteredUdpState {
    pub(crate) async fn forward_existing_managed_flow(
        &mut self,
        tasks: &mut JoinSet<ChainTask>,
        services: UdpRuntimeServices,
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
        let upstream = flow
            .outbound
            .upstream()
            .map(|value| (value.server.to_owned(), value.port));
        let (stage, result) = match connection {
            #[cfg(feature = "managed-stream-runtime")]
            RegisteredUdpConnection::Logical(connection) => {
                return connection.send(&flow.session, payload).await
            }
            #[cfg(feature = "upstream-association-runtime")]
            RegisteredUdpConnection::Upstream(_) => {
                return Err(unavailable(
                    "upstream association flows are handled by generic UDP dispatch",
                ))
            }
            #[cfg(feature = "managed-stream-runtime")]
            RegisteredUdpConnection::Tuple { connection, stage } => {
                connection.spawn_response_bridge(tasks, flow.session.id);
                (
                    stage,
                    connection
                        .send(&flow.session.target, flow.session.port, payload)
                        .await,
                )
            }
            #[cfg(feature = "managed-datagram-runtime")]
            RegisteredUdpConnection::Datagram { connection, stage } => (
                stage,
                connection
                    .send_datagram(
                        tasks,
                        Some(services),
                        flow.session.id,
                        &flow.session.target,
                        flow.session.port,
                        payload,
                    )
                    .await,
            ),
        };
        result.map_err(|error| FlowFailure {
            stage,
            error,
            upstream,
        })
    }
}
fn unavailable(message: &'static str) -> FlowFailure {
    FlowFailure {
        stage: "udp_protocol_forward",
        error: zero_engine::EngineError::Io(std::io::Error::other(message)),
        upstream: None,
    }
}
