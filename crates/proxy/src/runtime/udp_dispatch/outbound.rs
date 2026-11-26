mod retry;
use zero_core::Session;
use zero_engine::{EngineError, ResolvedOutbound, RouteMode};
use zero_platform_tokio::contract::BoxFuture;

use crate::inventory::{PreparedUdpLeafCandidate, PreparedUdpOutbound};
use crate::protocol_registry::UdpAdapterContext;

use super::{FlowFailure, FlowStartResult, UdpDispatch};

pub(crate) fn start_udp_resolved_outbound<'a>(
    dispatch: &'a mut UdpDispatch,
    ctx: UdpAdapterContext<'a>,
    session: &'a Session,
    resolved: ResolvedOutbound<'a>,
    payload: &'a [u8],
    mode: RouteMode,
) -> BoxFuture<'a, Result<FlowStartResult, FlowFailure>> {
    // Keep cold route/connector state off every enclosing UDP receive poll.
    // This future still runs in the original accepted task and Scope.
    Box::pin(async move {
        let prepared = ctx.runtime_services().protocols().prepare_udp_outbound(
            ctx.clone(),
            session,
            &resolved,
            payload,
            mode,
        )?;
        execute_prepared_udp_outbound(dispatch, ctx, session, payload, prepared).await
    })
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
            let tag = prepared.target.tag().to_owned();
            let upstream = prepared
                .target
                .endpoint()
                .map(|(server, port)| (server.to_owned(), port));
            let services = ctx.runtime_services().with_outbound_io(&tag);
            let execution = services
                .datagram_execution(&dispatch.inbound_tag, session)
                .map_err(|error| FlowFailure {
                    stage: "udp_datagram_relay",
                    error,
                    upstream: upstream.clone(),
                })?;
            let connection = crate::runtime::udp_flow::registered::LogicalConnection::open_relay(
                &execution, *prepared, session,
            )
            .await
            .map_err(|error| FlowFailure {
                stage: "udp_datagram_relay",
                error,
                upstream: upstream.clone(),
            })?;
            dispatch
                .flow_start_context()
                .start_logical(tag, upstream, session, payload, connection)
                .await
        }
        PreparedUdpOutbound::Relay(prepared) => {
            Box::pin(prepared.execute(dispatch, ctx, session, payload)).await
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

pub(crate) fn execute_prepared_udp_candidate<'a>(
    dispatch: &'a mut UdpDispatch,
    ctx: UdpAdapterContext<'a>,
    session: &'a Session,
    payload: &'a [u8],
    prepared: PreparedUdpLeafCandidate,
) -> BoxFuture<'a, Result<FlowStartResult, FlowFailure>> {
    Box::pin(async move {
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(native) = dispatch.runtime.native() {
            let covered = match &prepared {
                PreparedUdpLeafCandidate::Block { .. } | PreparedUdpLeafCandidate::Direct(_) => {
                    // TUN QUIC sniffing sets `session.target` to the recovered
                    // SNI domain while preserving the original literal IP in
                    // `session.direct_target`, so no DNS lookup occurs.
                    !matches!(
                        session.effective_direct_target(),
                        zero_core::Address::Domain(_)
                    )
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
            PreparedUdpLeafCandidate::Direct(target) => {
                super::direct::start(dispatch, ctx.runtime_services(), session, payload, target)
                    .await
            }
            #[cfg(feature = "managed-stream-runtime")]
            PreparedUdpLeafCandidate::DatagramFlow(target) => {
                let tag = target.tag();
                let endpoint = target
                    .endpoint()
                    .map(|(server, port)| (server.to_owned(), port));
                let services = ctx.runtime_services().with_outbound_io(tag);
                let execution = services
                    .datagram_execution(&dispatch.inbound_tag, session)
                    .map_err(|error| FlowFailure {
                        stage: "udp_datagram_open",
                        error,
                        upstream: endpoint.clone(),
                    })?;
                let connection =
                    crate::runtime::udp_flow::registered::LogicalConnection::open_datagram(
                        &execution, &target, session,
                    )
                    .await
                    .map_err(|error| FlowFailure {
                        stage: "udp_datagram_open",
                        error,
                        upstream: endpoint.clone(),
                    })?;
                dispatch
                    .flow_start_context()
                    .start_logical(tag.into(), endpoint, session, payload, connection)
                    .await
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
    })
}
