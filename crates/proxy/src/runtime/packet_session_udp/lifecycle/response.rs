use crate::runtime::udp_socket::DirectUdpResponseSource;

use tokio::time::Instant as TokioInstant;
use tracing::warn;
use zero_engine::EngineError;

use super::failure::handle_runtime_failure;
use super::relay::PacketSessionUdpLoopContext;
use crate::runtime::packet_session_udp::contract::PacketSessionUdpHandler;
use crate::runtime::udp_delivery::{
    record_chain_udp_response_parts, record_direct_udp_response_parts, write_chain_response,
    write_direct_response,
};
#[cfg(feature = "upstream-association-runtime")]
use crate::runtime::udp_delivery::{
    record_upstream_udp_response_received, write_upstream_response,
};
use crate::runtime::udp_dispatch::UdpDispatch;
use crate::runtime::udp_flow::packet_path::ChainTask;
#[cfg(feature = "upstream-association-runtime")]
use crate::runtime::udp_flow::response::UpstreamUdpResponse;

pub(super) type ChainUdpResponseResult = Result<ChainTask, tokio::task::JoinError>;

pub(super) async fn handle_direct_response<H>(
    context: &PacketSessionUdpLoopContext<'_>,
    handler: &mut H,
    dispatch: &UdpDispatch,
    last_activity: &mut TokioInstant,
    sender: DirectUdpResponseSource,
    payload: &[u8],
) -> Result<(), EngineError>
where
    H: PacketSessionUdpHandler,
{
    *last_activity = TokioInstant::now();
    let response =
        record_direct_udp_response_parts(context.runtime.services(), dispatch, sender, payload);
    if let Err(error) = write_direct_response(&response, || async {
        handler
            .write_response_for_target(&response.target, response.port, response.payload)
            .await
    })
    .await
    {
        return handle_runtime_failure(
            handler,
            context.failure_policy,
            context.inbound_tag,
            context.protocol,
            "packet session udp direct response encode failed",
            error.into(),
        )
        .await;
    }

    Ok(())
}

#[cfg(feature = "upstream-association-runtime")]
pub(super) async fn handle_upstream_response<H>(
    context: &PacketSessionUdpLoopContext<'_>,
    handler: &mut H,
    dispatch: &mut UdpDispatch,
    last_activity: &mut TokioInstant,
    upstream: Result<UpstreamUdpResponse, EngineError>,
) -> Result<(), EngineError>
where
    H: PacketSessionUdpHandler,
{
    match upstream {
        Ok(packet) => {
            *last_activity = TokioInstant::now();
            let response = record_upstream_udp_response_received(
                context.runtime.services(),
                dispatch,
                context.timeout,
                packet,
            );
            if let Err(error) = write_upstream_response(&response, || async {
                handler
                    .write_response_for_target(&response.target, response.port, &response.payload)
                    .await
            })
            .await
            {
                return handle_runtime_failure(
                    handler,
                    context.failure_policy,
                    context.inbound_tag,
                    context.protocol,
                    "packet session udp upstream response encode failed",
                    error.into(),
                )
                .await;
            }
        }
        Err(error) => warn!(
            error = %error,
            protocol = context.protocol,
            "packet session udp upstream recv failed"
        ),
    }

    Ok(())
}

pub(super) async fn handle_chain_result<H>(
    context: &PacketSessionUdpLoopContext<'_>,
    handler: &mut H,
    dispatch: &UdpDispatch,
    last_activity: &mut TokioInstant,
    chain_result: ChainUdpResponseResult,
) -> Result<(), EngineError>
where
    H: PacketSessionUdpHandler,
{
    match chain_result {
        Ok(Ok((target, port, payload, session_id))) => {
            *last_activity = TokioInstant::now();
            let response = record_chain_udp_response_parts(
                context.runtime.services(),
                dispatch,
                target,
                port,
                payload,
                session_id,
            );
            if let Err(error) = write_chain_response(&response, || async {
                handler
                    .write_response_for_target(&response.target, response.port, &response.payload)
                    .await
            })
            .await
            {
                return handle_runtime_failure(
                    handler,
                    context.failure_policy,
                    context.inbound_tag,
                    context.protocol,
                    "packet session udp chain response encode failed",
                    error.into(),
                )
                .await;
            }
        }
        Ok(Err(error)) => warn!(
            error = %error,
            protocol = context.protocol,
            "packet session udp chain response failed"
        ),
        Err(error) => warn!(
            error = %error,
            protocol = context.protocol,
            "packet session udp chain task panicked"
        ),
    }

    Ok(())
}
