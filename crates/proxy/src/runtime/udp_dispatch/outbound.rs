mod retry;
use zero_core::Session;
use zero_engine::{EngineError, ResolvedOutbound, RouteMode};

use crate::inventory::{PreparedUdpLeafCandidate, PreparedUdpOutbound};
use crate::protocol_registry::UdpAdapterContext;

use super::{FlowFailure, FlowStartResult, UdpDispatch};

pub(crate) async fn start_udp_resolved_outbound(
    dispatch: &mut UdpDispatch,
    ctx: UdpAdapterContext<'_>,
    session: &Session,
    resolved: ResolvedOutbound<'_>,
    payload: &[u8],
    mode: RouteMode,
) -> Result<FlowStartResult, FlowFailure> {
    let prepared = ctx.runtime_services().protocols().prepare_udp_outbound(
        ctx.clone(),
        session,
        &resolved,
        payload,
        mode,
    )?;
    execute_prepared_udp_outbound(dispatch, ctx, session, payload, prepared).await
}

async fn execute_prepared_udp_outbound(
    dispatch: &mut UdpDispatch,
    ctx: UdpAdapterContext<'_>,
    session: &Session,
    payload: &[u8],
    prepared: PreparedUdpOutbound<'_>,
) -> Result<FlowStartResult, FlowFailure> {
    match prepared {
        #[cfg(feature = "managed-stream-runtime")]
        PreparedUdpOutbound::DatagramRelay(prepared) => {
            #[cfg(feature = "raw-ip-runtime")]
            if let Some(native) = dispatch.runtime.native() {
                native
                    .declare_gateway(false)
                    .map_err(|source| FlowFailure {
                        stage: "native_gateway_admission",
                        error: std::io::Error::other(source).into(),
                        upstream: None,
                    })?;
            }
            let (tag, server, port) = prepared.target.endpoint();
            let (tag, upstream) = (tag.to_owned(), (server.to_owned(), port));
            let services = ctx.runtime_services().with_outbound_io(&tag);
            let execution = services
                .datagram_execution(&dispatch.inbound_tag, session)
                .map_err(|error| FlowFailure {
                    stage: "udp_datagram_relay",
                    error,
                    upstream: Some(upstream.clone()),
                })?;
            let connection = crate::runtime::udp_flow::registered::LogicalConnection::open_relay(
                &execution, *prepared, session,
            )
            .await
            .map_err(|error| FlowFailure {
                stage: "udp_datagram_relay",
                error,
                upstream: Some(upstream.clone()),
            })?;
            dispatch
                .flow_start_context()
                .start_logical(tag, Some(upstream), session, payload, connection)
                .await
        }
        PreparedUdpOutbound::Relay(prepared) => {
            prepared.execute(dispatch, ctx, session, payload).await
        }
        PreparedUdpOutbound::Single(prepared) => {
            execute_prepared_udp_candidate(dispatch, ctx, session, payload, prepared).await
        }
        PreparedUdpOutbound::Fallback(candidates) => {
            let mut last_failure = None;

            for prepared in candidates {
                match execute_prepared_udp_candidate(
                    dispatch,
                    ctx.clone(),
                    session,
                    payload,
                    prepared,
                )
                .await
                {
                    Ok(result) => return Ok(result),
                    Err(failure) => {
                        if !retry::permits_fallback(&failure.error) {
                            return Err(failure);
                        }
                        last_failure = Some(failure);
                    }
                }
            }

            Err(last_failure.unwrap_or_else(|| FlowFailure {
                stage: "fallback_exhausted",
                error: EngineError::Io(std::io::Error::other("all fallback outbounds failed")),
                upstream: None,
            }))
        }
    }
}

pub(crate) async fn execute_prepared_udp_candidate(
    dispatch: &mut UdpDispatch,
    ctx: UdpAdapterContext<'_>,
    session: &Session,
    payload: &[u8],
    prepared: PreparedUdpLeafCandidate<'_>,
) -> Result<FlowStartResult, FlowFailure> {
    #[cfg(feature = "raw-ip-runtime")]
    if let Some(native) = dispatch.runtime.native() {
        let covered = match &prepared {
            PreparedUdpLeafCandidate::Block { .. } => {
                !matches!(session.target, zero_core::Address::Domain(_))
            }
            PreparedUdpLeafCandidate::Flow(operation) => {
                operation.controlled_native_coverage(session)
            }
            #[cfg(feature = "upstream-association-runtime")]
            PreparedUdpLeafCandidate::Datagram(_) => false,
            #[cfg(feature = "managed-stream-runtime")]
            PreparedUdpLeafCandidate::DatagramFlow(_) => false,
        } && dispatch.bound_direct_registration().is_some();
        native
            .declare_gateway(covered)
            .map_err(|source| FlowFailure {
                stage: "native_gateway_admission",
                error: std::io::Error::other(source).into(),
                upstream: None,
            })?;
    }
    match prepared {
        PreparedUdpLeafCandidate::Block { tag } => Ok(FlowStartResult::Blocked { tag }),
        #[cfg(feature = "managed-stream-runtime")]
        PreparedUdpLeafCandidate::DatagramFlow(target) => {
            let (tag, server, port) = target.endpoint();
            let services = ctx.runtime_services().with_outbound_io(tag);
            let execution = services
                .datagram_execution(&dispatch.inbound_tag, session)
                .map_err(|error| FlowFailure {
                    stage: "udp_datagram_open",
                    error,
                    upstream: Some((server.into(), port)),
                })?;
            let connection =
                crate::runtime::udp_flow::registered::LogicalConnection::open_datagram(
                    &execution, &target, session,
                )
                .await
                .map_err(|error| FlowFailure {
                    stage: "udp_datagram_open",
                    error,
                    upstream: Some((server.into(), port)),
                })?;
            dispatch
                .flow_start_context()
                .start_logical(
                    tag.into(),
                    Some((server.into(), port)),
                    session,
                    payload,
                    connection,
                )
                .await
        }
        PreparedUdpLeafCandidate::Flow(operation) => {
            operation.execute(dispatch, ctx, session, payload).await
        }
        #[cfg(feature = "upstream-association-runtime")]
        PreparedUdpLeafCandidate::Datagram(target) => {
            use crate::runtime::udp_flow::registered::UpstreamAssociationTarget;
            let services = ctx
                .runtime_services()
                .with_outbound_io(target.outbound_tag());
            dispatch
                .start_datagram(services, target, session, payload)
                .await
        }
    }
}
