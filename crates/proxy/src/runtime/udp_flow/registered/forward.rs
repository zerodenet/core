use super::{state::RegisteredUdpConnection, RegisteredUdpState};
use crate::protocol_registry::UdpRuntimeServices;
use crate::runtime::udp_flow::managed::ManagedExistingFlowForward;
use crate::runtime::udp_flow::packet_path::ChainTask;
use crate::runtime::udp_flow::result::FlowFailure;
use tokio::task::JoinSet;

impl RegisteredUdpState {
    pub(crate) async fn forward_existing_managed_flow(
        &mut self,
        _chain_tasks: &mut JoinSet<ChainTask>,
        _services: UdpRuntimeServices,
        request: ManagedExistingFlowForward<'_>,
    ) -> Result<usize, FlowFailure> {
        let (flow, _) = request;
        let Some(flow_ref) = flow.outbound.managed_flow() else {
            return Err(unavailable(
                "protocol UDP flow has no managed resume reference",
            ));
        };
        let Some(connection) = self.connections.get(&flow_ref).cloned() else {
            return Err(unavailable("managed UDP flow resume was dropped"));
        };
        let resume = match connection {
            #[cfg(feature = "managed-stream-runtime")]
            RegisteredUdpConnection::Logical(connection) => {
                return connection.send(&request.0.session, request.1).await;
            }
            #[cfg(feature = "upstream-association-runtime")]
            RegisteredUdpConnection::Upstream(_) => {
                return Err(unavailable(
                    "upstream association flows are handled by generic UDP dispatch",
                ))
            }
            RegisteredUdpConnection::Legacy(resume) => resume,
        };

        #[cfg(any(
            feature = "managed-stream-runtime",
            feature = "managed-datagram-runtime"
        ))]
        if let Some(result) = self
            .managed
            .forward_existing_flow(_chain_tasks, _services, request, resume)
            .await?
        {
            return Ok(result);
        }

        Err(unavailable(
            "protocol UDP flow snapshot has no compiled forward handler",
        ))
    }
}

fn unavailable(message: &'static str) -> FlowFailure {
    FlowFailure {
        stage: "udp_protocol_forward",
        error: zero_engine::EngineError::Io(std::io::Error::other(message)),
        upstream: None,
    }
}
