use tokio::select;
use zero_core::DatagramUdpResponder;
use zero_engine::EngineError;

use super::read::process_datagram_read;
use super::relay::DatagramUdpLoopContext;
use super::response::{handle_chain_result, handle_direct_response};
use crate::runtime::udp_delivery::write_optional_upstream_response;
use crate::runtime::udp_delivery::{record_upstream_udp_response_received, wait_for_upstream_idle};
use crate::runtime::udp_dispatch::UdpDispatch;

pub(super) async fn run_loop<S, R>(
    context: &DatagramUdpLoopContext<'_>,
    source: &S,
    responder: &mut R,
    dispatch: &mut UdpDispatch,
    direct_buf: &mut [u8],
    upstream_buf: &mut [u8],
) -> Result<(), EngineError>
where
    S: Send,
    R: DatagramUdpResponder<S>,
{
    loop {
        let (direct_sock, upstream_udp, upstream_idle_deadline, chain_tasks, cancel_rx) =
            dispatch.poll_refs();
        select! {
            read = responder.read_inbound_dispatch(source) => {
                if !process_datagram_read::<S, R>(context, dispatch, responder, read).await {
                    break;
                }
            }
            recv = direct_sock.recv_from_addr(direct_buf) => {
                let (n, sender) = recv?;
                handle_direct_response(context, source, responder, dispatch, sender, &direct_buf[..n]).await;
            }
            upstream = upstream_udp.recv_response(upstream_buf) => {
                match upstream {
                    Ok(pkt) => {
                        let response = record_upstream_udp_response_received(
                            context.runtime.services(),
                            dispatch,
                            context.runtime.services().udp_upstream_idle_timeout(),
                            pkt,
                        );
                        let _ = write_optional_upstream_response(&response, || async {
                            responder
                                .write_response_for_session(
                                    source,
                                    response.accounting.session_id(),
                                    &response.target,
                                    response.port,
                                    &response.payload,
                                )
                                .await
                        })
                        .await;
                    }
                    Err(error) => tracing::warn!(error = %error, "datagram udp upstream response error"),
                }
            }
            _ = wait_for_upstream_idle(upstream_idle_deadline) => {}
            Some(chain_result) = chain_tasks.join_next() => {
                handle_chain_result(context, dispatch, source, responder, chain_result).await;
            }
            Some(session_id) = cancel_rx.recv() => {
                if dispatch.finish_cancelled_flow(session_id) {
                    break;
                }
            }
        }
    }

    Ok(())
}
