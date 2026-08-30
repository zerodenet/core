use std::future::Future;
use std::pin::Pin;

use zero_core::Session;

use super::contract::PreparedUdpFlowOperation;
use crate::protocol_registry::{UdpAdapterContext, UdpRuntimeServices};
use crate::runtime::udp_dispatch::UdpDispatch;
use crate::runtime::udp_flow::outbound::UdpFlowOutbound;
use crate::runtime::udp_flow::result::{FlowFailure, FlowStartResult};

pub(crate) struct DirectUdpFlowOperation {
    pub(crate) tag: String,
}

impl PreparedUdpFlowOperation for DirectUdpFlowOperation {
    fn execute<'a>(
        self: Box<Self>,
        dispatch: &'a mut UdpDispatch,
        ctx: UdpAdapterContext<'a>,
        session: &'a Session,
        payload: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<FlowStartResult, FlowFailure>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            execute_direct_udp_operation(
                dispatch,
                ctx.runtime_services(),
                session,
                payload,
                PreparedDirectUdpOperation { tag: &self.tag },
            )
            .await
        })
    }
}

struct PreparedDirectUdpOperation<'a> {
    tag: &'a str,
}

async fn execute_direct_udp_operation(
    dispatch: &mut UdpDispatch,
    services: UdpRuntimeServices,
    session: &Session,
    payload: &[u8],
    operation: PreparedDirectUdpOperation<'_>,
) -> Result<FlowStartResult, FlowFailure> {
    let candidates = match services.resolve_direct_targets(session).await {
        Ok(candidates) => candidates,
        Err(error) => {
            services.record_session_network(
                session.id,
                services.direct_resolution_failure_observation(session),
            );
            return Err(FlowFailure {
                stage: "resolve_udp_target",
                error,
                upstream: None,
            });
        }
    };
    let (sent, target_addr) = dispatch
        .send_new_direct_packet(&session.target, candidates.udp_candidates(), payload)
        .await
        .map_err(|error| FlowFailure {
            stage: "udp_direct_send",
            error,
            upstream: None,
        })?;
    services.record_session_network(
        session.id,
        services.direct_udp_network_observation(&candidates, target_addr),
    );
    Ok(FlowStartResult::Flow {
        outbound: Box::new(UdpFlowOutbound::Direct {
            tag: operation.tag.to_owned(),
            target_addr,
        }),
        tx_bytes: sent as u64,
    })
}
