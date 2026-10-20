use zero_engine::EngineError;
use zero_platform_tokio::TokioDatagramSocket;

use super::relay::UdpAssociationLoopContext;
use crate::logging::log_udp_upstream_association_dropped;
use crate::runtime::udp_association::contract::UdpAssociationHandler;
use crate::runtime::udp_delivery::log_completed_udp_flow;
use crate::runtime::udp_delivery::{
    record_chain_udp_response_parts, record_upstream_udp_response_received,
};
use crate::runtime::udp_delivery::{
    write_chain_response as write_chain_udp_response,
    write_upstream_response as write_upstream_udp_response,
};
use crate::runtime::udp_dispatch::UdpDispatch;
use crate::runtime::udp_flow::response::UpstreamUdpResponse;

pub(super) type ChainAssociationResult = crate::runtime::udp_flow::response_join::ObservedChainJoin;

pub(super) fn finish_dispatch(dispatch: UdpDispatch) {
    for completed in dispatch.finish_all() {
        log_completed_udp_flow(completed);
    }
}

pub(super) async fn handle_upstream_response<H>(
    context: &UdpAssociationLoopContext<'_>,
    dispatch: &mut UdpDispatch,
    handler: &mut H,
    relay: &TokioDatagramSocket,
    upstream: Result<UpstreamUdpResponse, EngineError>,
) -> Result<(), EngineError>
where
    H: UdpAssociationHandler,
{
    match upstream {
        Ok(response) => {
            let response = record_upstream_udp_response_received(
                context.runtime.services(),
                dispatch,
                context.runtime.services().udp_upstream_idle_timeout(),
                response,
            );
            write_upstream_udp_response(&response, || async {
                handler.write_upstream_response(relay, &response).await
            })
            .await?;
        }
        Err(error) => {
            if let Some(closed) = dispatch.drop_upstream_association() {
                context
                    .runtime
                    .services()
                    .record_udp_upstream_recv_failure();
                log_udp_upstream_association_dropped(
                    context.inbound_tag,
                    &closed.outbound_tag,
                    &closed.server,
                    closed.port,
                    &error,
                );
            }
        }
    }

    Ok(())
}

pub(super) async fn handle_chain_result<H>(
    context: &UdpAssociationLoopContext<'_>,
    dispatch: &UdpDispatch,
    handler: &mut H,
    relay: &TokioDatagramSocket,
    chain_result: ChainAssociationResult,
) where
    H: UdpAssociationHandler,
{
    match chain_result {
        Ok(Ok((target, port, payload, session_id))) => {
            let response = record_chain_udp_response_parts(
                context.runtime.services(),
                dispatch,
                target,
                port,
                payload,
                session_id,
            );
            if let Err(error) = write_chain_udp_response(&response, || async {
                handler.write_chain_response(relay, &response).await
            })
            .await
            {
                tracing::warn!(
                    inbound_tag = context.inbound_tag,
                    target = ?response.target,
                    port = response.port,
                    error = ?error,
                    "failed to send UDP chain response to client"
                );
            }
        }
        Ok(Err(error)) => {
            tracing::warn!(error = %error, "chain upstream read error");
        }
        Err(join_err) => {
            tracing::warn!(error = %join_err, "chain response task panicked");
        }
    }
}
