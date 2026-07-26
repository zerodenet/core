use tokio::time::Instant as TokioInstant;
use tracing::warn;

use super::relay::PacketSessionUdpLoopContext;
use crate::runtime::packet_session_udp::contract::{
    PacketSessionUdpReadFailure, PacketSessionUdpReadFailureAction, PacketSessionUdpReadResult,
};
use crate::runtime::udp_dispatch::UdpDispatch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PacketSessionUdpReadControl {
    Continue,
    End,
}

pub(super) async fn process_packet_session_read(
    context: &PacketSessionUdpLoopContext<'_>,
    dispatch: &mut UdpDispatch,
    last_activity: &mut TokioInstant,
    read: Result<PacketSessionUdpReadResult, PacketSessionUdpReadFailure>,
) -> PacketSessionUdpReadControl {
    match read {
        Ok(PacketSessionUdpReadResult::Dispatch(inbound_dispatch)) => {
            *last_activity = TokioInstant::now();
            if let Err(error) = context
                .runtime
                .dispatch_inbound_packet(dispatch, &inbound_dispatch, context.auth, None)
                .await
            {
                warn!(
                    error = %error,
                    protocol = context.protocol,
                    "packet session udp dispatch failed"
                );
            }
            PacketSessionUdpReadControl::Continue
        }
        Ok(PacketSessionUdpReadResult::End) => PacketSessionUdpReadControl::End,
        Err(failure) => {
            warn!(
                error = %failure.error,
                protocol = context.protocol,
                "packet session udp inbound read/decode error"
            );
            match failure.action {
                #[cfg(feature = "managed-stream-runtime")]
                PacketSessionUdpReadFailureAction::Continue => {
                    PacketSessionUdpReadControl::Continue
                }
                PacketSessionUdpReadFailureAction::End => PacketSessionUdpReadControl::End,
            }
        }
    }
}
