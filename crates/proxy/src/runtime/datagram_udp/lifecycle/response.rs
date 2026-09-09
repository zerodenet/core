use crate::runtime::udp_socket::DirectUdpResponseSource;

use zero_core::DatagramUdpResponder;
use zero_engine::EngineError;

use super::relay::DatagramUdpLoopContext;
use crate::runtime::udp_delivery::{
    log_completed_udp_flow, record_chain_udp_response_parts, record_direct_udp_response_parts,
    write_optional_chain_response, write_optional_direct_response,
};
use crate::runtime::udp_dispatch::UdpDispatch;

pub(super) type ChainUdpResponseResult = Result<
    Result<(zero_core::Address, u16, Vec<u8>, Option<u64>), EngineError>,
    tokio::task::JoinError,
>;

pub(super) fn finish_dispatch(dispatch: UdpDispatch) {
    for completed in dispatch.finish_all() {
        log_completed_udp_flow(completed);
    }
}

pub(super) async fn handle_direct_response<S, R>(
    context: &DatagramUdpLoopContext<'_>,
    source: &S,
    responder: &mut R,
    dispatch: &UdpDispatch,
    sender: DirectUdpResponseSource,
    payload: &[u8],
) where
    S: Send,
    R: DatagramUdpResponder<S>,
{
    let response =
        record_direct_udp_response_parts(context.runtime.services(), dispatch, sender, payload);
    let _ = write_optional_direct_response(&response, || async {
        responder
            .write_response_for_session(
                source,
                response.accounting.session_id(),
                &response.target,
                response.port,
                response.payload,
            )
            .await
    })
    .await;
}

pub(super) async fn handle_chain_result<S, R>(
    context: &DatagramUdpLoopContext<'_>,
    dispatch: &UdpDispatch,
    source: &S,
    responder: &mut R,
    chain_result: ChainUdpResponseResult,
) where
    S: Send,
    R: DatagramUdpResponder<S>,
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
            let _ = write_optional_chain_response(&response, || async {
                responder
                    .write_response_for_session(
                        source,
                        session_id,
                        &response.target,
                        response.port,
                        &response.payload,
                    )
                    .await
            })
            .await;
        }
        Ok(Err(error)) => tracing::warn!(error = %error, "datagram udp chain response error"),
        Err(error) => tracing::warn!(error = %error, "datagram udp chain task panicked"),
    }
}
