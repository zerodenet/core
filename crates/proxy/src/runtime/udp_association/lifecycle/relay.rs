use tokio::io::AsyncReadExt;
use tokio::select;
use zero_engine::EngineError;

use super::idle::handle_idle_timeout;
use super::response::{finish_dispatch, handle_chain_result, handle_upstream_response};
use crate::runtime::udp_association::contract::{
    UdpAssociationDatagramRequest, UdpAssociationHandler, UdpAssociationLoopRequest,
};
use crate::runtime::udp_delivery::wait_for_upstream_idle;
use crate::runtime::udp_ingress::UdpIngressRuntime;
use crate::transport::ClientStream;

pub(super) struct UdpAssociationLoopContext<'a> {
    pub(super) runtime: &'a UdpIngressRuntime,
    pub(super) inbound_tag: &'a str,
}

pub(crate) async fn run_udp_association_loop<S, H>(
    request: UdpAssociationLoopRequest<'_, S, H>,
) -> Result<(), EngineError>
where
    S: ClientStream,
    H: UdpAssociationHandler,
{
    let UdpAssociationLoopRequest {
        runtime,
        client,
        inbound_tag,
        relay,
        mut pending_control_traffic,
        mut handler,
    } = request;

    // Claim before any dispatcher/upstream open. A concurrent listener stop
    // either sees this guard, or rejects this late claim without opening I/O.
    let stop_guard = match runtime.claim_listener_stop() {
        Ok(guard) => guard,
        Err(error) if matches!(&error, EngineError::Io(io) if io.kind() == std::io::ErrorKind::Interrupted) => {
            return Ok(())
        }
        Err(error) => return Err(error),
    };
    let mut dispatch = runtime.new_dispatch(inbound_tag).await?;
    let context = UdpAssociationLoopContext {
        runtime: &runtime,
        inbound_tag,
    };
    let mut control_probe = [0_u8; 1];
    let mut packet = vec![0_u8; 64 * 1024];
    let mut direct_buf = vec![0_u8; 64 * 1024];
    let mut upstream_buf = vec![0_u8; 64 * 1024];

    let result = async {
        loop {
            let (direct_sock, upstream_udp, idle_deadline, chain_tasks, cancel_rx) =
                dispatch.poll_refs();

            select! {
                _ = async {
                    match &stop_guard {
                        Some(guard) => guard.cancelled().await,
                        None => std::future::pending().await,
                    }
                } => break Ok(()),
                control = client.read(&mut control_probe) => {
                    match control {
                        Ok(0) => break Ok(()),
                        Ok(_) => break Ok(()),
                        Err(error) => return Err(error.into()),
                    }
                }
                recv = relay.recv_from_addr(&mut packet) => {
                    let (read, sender) = recv?;
                    handler
                        .handle_client_datagram(UdpAssociationDatagramRequest {
                            runtime: context.runtime,
                            dispatch: &mut dispatch,
                            relay: &relay,
                            pending_control_traffic: &mut pending_control_traffic,
                            sender,
                            payload: &packet[..read],
                        })
                        .await?;
                }
                recv = direct_sock.recv_from_addr(&mut direct_buf) => {
                    let (read, sender) = recv?;
                    if let Err(error) = handler
                        .write_direct_response(
                            context.runtime,
                            &dispatch,
                            &relay,
                            sender,
                            &direct_buf[..read],
                        )
                        .await
                    {
                        tracing::warn!(
                            inbound_tag = context.inbound_tag,
                            error = %error,
                            "failed to forward direct UDP response"
                        );
                    }
                }
                upstream = upstream_udp.recv_response(&mut upstream_buf) => {
                    handle_upstream_response(&context, &mut dispatch, &mut handler, &relay, upstream).await?;
                }
                Some(chain_result) = chain_tasks.join_next() => {
                    handle_chain_result(&context, &dispatch, &mut handler, &relay, chain_result).await;
                }
                _ = wait_for_upstream_idle(idle_deadline) => {
                    handle_idle_timeout(&context, &mut dispatch);
                }
                Some(session_id) = cancel_rx.recv() => {
                    if dispatch.finish_cancelled_flow(session_id) {
                        break Ok(());
                    }
                }
            }
        }
    }
    .await;
    if result.is_err() {
        // Error exits remain Dropped; normal EOF/listener stop uses Closed.
        // Both paths finish flow bookkeeping before the consumer guard exits.
        while dispatch.drop_upstream_association().is_some() {}
    }
    finish_dispatch(dispatch);
    result
}
