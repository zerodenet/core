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
    pub(crate) policy_tag: Option<String>,
    pub(crate) dial_policy: zero_traits::DialPolicy,
    pub(crate) dial_generation: u64,
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
                PreparedDirectUdpOperation {
                    tag: &self.tag,
                    policy: crate::runtime::udp_socket::DirectUdpPolicy {
                        tag: self.policy_tag.clone(),
                        dial_policy: self.dial_policy.clone(),
                        generation: self.dial_generation,
                    },
                },
            )
            .await
        })
    }
}

struct PreparedDirectUdpOperation<'a> {
    tag: &'a str,
    policy: crate::runtime::udp_socket::DirectUdpPolicy,
}

async fn execute_direct_udp_operation(
    dispatch: &mut UdpDispatch,
    services: UdpRuntimeServices,
    session: &Session,
    payload: &[u8],
    operation: PreparedDirectUdpOperation<'_>,
) -> Result<FlowStartResult, FlowFailure> {
    services
        .network()
        .ensure_direct_policy_current(&operation.policy)
        .map_err(|error| FlowFailure {
            stage: "udp_direct_policy",
            error,
            upstream: None,
        })?;
    let candidates = match services
        .resolve_direct_targets(session, &operation.policy.dial_policy)
        .await
    {
        Ok(candidates) => candidates,
        Err(error) => {
            services.record_session_network(
                session.id,
                services
                    .direct_resolution_failure_observation(session, &operation.policy.dial_policy),
            );
            return Err(FlowFailure {
                stage: "resolve_udp_target",
                error,
                upstream: None,
            });
        }
    };
    let sent = dispatch
        .send_new_direct_packet(
            session.id,
            &session.target,
            candidates.udp_candidates(),
            &operation.policy,
            payload,
        )
        .await
        .map_err(|error| FlowFailure {
            stage: "udp_direct_send",
            error,
            upstream: None,
        })?;
    services.record_session_network(
        session.id,
        services.direct_udp_network_observation(
            &candidates,
            sent.target,
            sent.local,
            &sent.selection,
        ),
    );
    Ok(FlowStartResult::Flow {
        outbound: Box::new(UdpFlowOutbound::Direct {
            tag: operation.tag.to_owned(),
            target_addr: sent.target,
            policy: operation.policy,
        }),
        tx_bytes: sent.sent as u64,
    })
}
