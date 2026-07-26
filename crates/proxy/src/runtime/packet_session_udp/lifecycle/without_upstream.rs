use tokio::select;
use tokio::time::Instant as TokioInstant;
use zero_engine::EngineError;

use super::failure::handle_runtime_failure;
use super::read::{process_packet_session_read, PacketSessionUdpReadControl};
use super::relay::{PacketSessionUdpLoopContext, PacketSessionUdpLoopExit};
use super::response::{handle_chain_result, handle_direct_response};
use crate::runtime::packet_session_udp::contract::PacketSessionUdpHandler;

pub(super) async fn run_loop<H>(
    context: &PacketSessionUdpLoopContext<'_>,
    handler: &mut H,
    dispatch: &mut crate::runtime::udp_dispatch::UdpDispatch,
    last_activity: &mut TokioInstant,
    direct_buf: &mut [u8],
) -> Result<PacketSessionUdpLoopExit, EngineError>
where
    H: PacketSessionUdpHandler,
{
    loop {
        let (direct_sock, chain_tasks, cancel_rx) = dispatch.poll_sockets();

        select! {
            _ = tokio::time::sleep_until(*last_activity + context.timeout) => {
                tracing::info!(
                    inbound_tag = context.inbound_tag,
                    protocol = context.protocol,
                    "packet session udp relay idle timeout"
                );
                return Ok(PacketSessionUdpLoopExit::IdleTimeout);
            }
            read = handler.read_inbound_dispatch() => {
                if process_packet_session_read(context, dispatch, last_activity, read).await
                    == PacketSessionUdpReadControl::End
                {
                    return Ok(PacketSessionUdpLoopExit::InboundEnded);
                }
            }
            recv = direct_sock.recv_from_addr(direct_buf) => {
                match recv {
                    Ok((n, sender)) => {
                        handle_direct_response(
                            context,
                            handler,
                            dispatch,
                            last_activity,
                            sender,
                            &direct_buf[..n],
                        )
                        .await?;
                    }
                    Err(error) => {
                        return handle_runtime_failure(
                            handler,
                            context.failure_policy,
                            context.inbound_tag,
                            context.protocol,
                            "packet session udp direct recv failed",
                            error.into(),
                        )
                        .await
                        .map(|_| PacketSessionUdpLoopExit::InboundEnded);
                    }
                }
            }
            Some(chain_result) = chain_tasks.join_next() => {
                handle_chain_result(context, handler, dispatch, last_activity, chain_result).await?;
            }
            Some(session_id) = cancel_rx.recv() => {
                if dispatch.finish_cancelled_flow(session_id) {
                    return Ok(PacketSessionUdpLoopExit::AssociationCancelled);
                }
            }
        }
    }
}
