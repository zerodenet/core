use zero_core::{DatagramUdpResponder, InboundUdpDispatch};

use super::relay::DatagramUdpLoopContext;
use crate::runtime::udp_dispatch::UdpDispatch;

pub(super) async fn process_datagram_read<S, R>(
    context: &DatagramUdpLoopContext<'_>,
    dispatch: &mut UdpDispatch,
    responder: &mut R,
    read: Result<Option<InboundUdpDispatch>, zero_core::Error>,
) -> bool
where
    S: Send,
    R: DatagramUdpResponder<S>,
{
    let inbound_dispatch = match read {
        Ok(Some(inbound_dispatch)) => inbound_dispatch,
        Ok(None) => return false,
        Err(error) => {
            tracing::warn!(error = %error, "datagram udp inbound read/decode error");
            return false;
        }
    };
    let auth = context.auth.or_else(|| responder.auth());
    match context
        .runtime
        .dispatch_inbound_packet(dispatch, &inbound_dispatch, auth, responder.client_addr())
        .await
    {
        // Blocked dispatches finish their business session without installing
        // a response flow. Publishing them as live mappings would retain TUN
        // associations that can never receive an answer.
        Ok(session_id) if dispatch.contains_session(session_id) => {
            responder.on_dispatch_success(session_id, &inbound_dispatch)
        }
        Ok(_) => {}
        Err(zero_engine::EngineError::AdmissionDenied { reason }) => {
            tracing::debug!(%reason, "datagram UDP flow start suppressed by backoff")
        }
        Err(error) => tracing::warn!(error = %error, "datagram udp dispatch failed"),
    }
    true
}
