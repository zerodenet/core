use zero_core::{InboundMuxUdpReadFailureAction, InboundMuxUdpRelay};

use crate::runtime::packet_session_udp::{
    PacketSessionUdpHandler, PacketSessionUdpReadFailure, PacketSessionUdpReadFailureAction,
    PacketSessionUdpReadResult,
};

pub(super) struct MuxPacketSessionUdpHandler<R> {
    pub(super) relay: R,
    pub(super) sniffing: Option<crate::runtime::sniff::udp::UdpSniffingState>,
    pub(super) relay_ended: bool,
    pub(super) deferred_failure: Option<PacketSessionUdpReadFailure>,
}

impl<R> PacketSessionUdpHandler for MuxPacketSessionUdpHandler<R>
where
    R: InboundMuxUdpRelay,
{
    async fn read_inbound_dispatch(
        &mut self,
    ) -> Result<PacketSessionUdpReadResult, PacketSessionUdpReadFailure> {
        loop {
            if let Some(dispatch) = self
                .sniffing
                .as_mut()
                .and_then(|sniffing| sniffing.pop_ready())
            {
                return Ok(PacketSessionUdpReadResult::Dispatch(dispatch));
            }
            if let Some(failure) = self.deferred_failure.take() {
                return Err(failure);
            }
            if self
                .sniffing
                .as_mut()
                .is_some_and(|sniffing| sniffing.release_for_capacity())
            {
                continue;
            }
            if self.relay_ended {
                return Ok(PacketSessionUdpReadResult::End);
            }
            let read = if let Some(deadline) = self
                .sniffing
                .as_ref()
                .and_then(|sniffing| sniffing.next_deadline())
            {
                match tokio::time::timeout_at(
                    tokio::time::Instant::from_std(deadline),
                    self.relay.read_inbound_dispatch(),
                )
                .await
                {
                    Ok(read) => read,
                    Err(_) => {
                        if let Some(sniffing) = &mut self.sniffing {
                            sniffing.flush_expired(std::time::Instant::now());
                        }
                        continue;
                    }
                }
            } else {
                self.relay.read_inbound_dispatch().await
            };
            match read {
                Ok(Some(dispatch)) => {
                    if let Some(sniffing) = &mut self.sniffing {
                        sniffing.observe(dispatch).await;
                    } else {
                        return Ok(PacketSessionUdpReadResult::Dispatch(dispatch));
                    }
                }
                Ok(None) => {
                    self.relay_ended = true;
                    if let Some(sniffing) = &mut self.sniffing {
                        sniffing.flush_all();
                    }
                }
                Err(failure) => {
                    let failure = PacketSessionUdpReadFailure {
                        error: failure.error,
                        action: match failure.action {
                            InboundMuxUdpReadFailureAction::Continue => {
                                PacketSessionUdpReadFailureAction::Continue
                            }
                            InboundMuxUdpReadFailureAction::End => {
                                PacketSessionUdpReadFailureAction::End
                            }
                        },
                    };
                    if let Some(sniffing) = &mut self.sniffing {
                        sniffing.flush_all();
                        self.deferred_failure = Some(failure);
                    } else {
                        return Err(failure);
                    }
                }
            }
        }
    }

    async fn write_response_for_target(
        &mut self,
        target: &zero_core::Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, zero_core::Error> {
        self.relay.write_response_for_target(target, port, payload)
    }

    async fn finish(&mut self) -> Result<(), zero_core::Error> {
        self.relay.end_inbound_stream().map(|_| ())
    }
}
