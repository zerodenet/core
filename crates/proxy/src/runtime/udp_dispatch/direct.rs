//! Direct business egress uses the installed dispatcher original/provider.
#[cfg(feature = "managed-datagram-runtime")]
mod source;
use super::UdpDispatch;
use crate::inventory::DirectDatagramTarget;
use crate::protocol_registry::UdpRuntimeServices;
use crate::runtime::udp_flow::{
    outbound::UdpFlowOutbound,
    result::{FlowFailure, FlowStartResult},
};
#[cfg(all(test, feature = "managed-datagram-runtime"))]
pub(crate) use source::tests::fixture::Live as LiveDirectSource;
#[cfg(feature = "managed-datagram-runtime")]
pub(crate) use source::DirectSocketSource;
use zero_core::Session;

pub(super) async fn start(
    dispatch: &mut UdpDispatch,
    services: UdpRuntimeServices,
    session: &Session,
    payload: &[u8],
    target: DirectDatagramTarget,
) -> Result<FlowStartResult, FlowFailure> {
    #[cfg(feature = "managed-datagram-runtime")]
    dispatch
        .admit_bound_direct(&target, session)
        .await
        .map_err(|error| FlowFailure {
            stage: "udp_direct_bound_admission",
            error,
            upstream: None,
        })?;
    send_direct_datagram(
        dispatch,
        services,
        session,
        payload,
        PreparedDirectDatagram {
            tag: &target.tag,
            policy: crate::runtime::udp_socket::DirectUdpPolicy {
                tag: target.policy_tag,
                dial_policy: target.dial_policy,
                generation: target.dial_generation,
            },
        },
    )
    .await
}

struct PreparedDirectDatagram<'a> {
    tag: &'a str,
    policy: crate::runtime::udp_socket::DirectUdpPolicy,
}

async fn send_direct_datagram(
    dispatch: &mut UdpDispatch,
    services: UdpRuntimeServices,
    session: &Session,
    payload: &[u8],
    operation: PreparedDirectDatagram<'_>,
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
