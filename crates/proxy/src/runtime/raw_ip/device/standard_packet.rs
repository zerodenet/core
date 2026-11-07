//! Synchronous owned submission to the existing bounded endpoint queue.
use super::*;
use zero_core::packet::IpPacket;
use zero_platform_tokio::contract::{
    scope::TaskScopeIdentity, ChannelError, DeliveryState, Impact, Reason, SendFailure,
    SendReceipt, Stage,
};
pub(crate) struct PacketSubmission {
    pub(crate) source: returns::ReturnSource,
    pub(crate) replies: zero_stack::packet_output::PacketSender,
    pub(crate) observer: Option<Arc<dyn zero_traits::IoObserver>>,
    pub(crate) local: IpAddr,
    pub(crate) translate: bool,
    pub(crate) mtu: usize,
}
impl SharedRawIpDevice {
    pub(crate) fn queue_standard_reply(
        &self,
        replies: &zero_stack::packet_output::PacketSender,
        packet: zero_traits::PacketBuffer,
    ) -> Result<(), tokio::sync::mpsc::error::TrySendError<zero_traits::PacketBuffer>> {
        self.returns.queue_reply(replies, packet)
    }
    pub(crate) fn try_submit_standard_packet(
        &self,
        scope: &TaskScopeIdentity,
        original: &mut Option<IpPacket>,
        request: PacketSubmission,
    ) -> Result<SendReceipt, SendFailure> {
        self.check_standard_scope(scope).map_err(rejected)?;
        let frame = original
            .as_mut()
            .ok_or_else(|| rejected(error(Reason::Protocol)))?;
        let source = zero_stack::packet::ip_source(frame.payload())
            .ok_or_else(|| rejected(error(Reason::Protocol)))?;
        let permit = self
            .forwarded_packets
            .try_reserve()
            .map_err(|_| rejected(error(Reason::Capacity)))?;
        let wire = self
            .packet_wire_memory
            .ok_or_else(|| rejected(error(Reason::InvalidConfig)))?;
        if frame.payload().len() > request.mtu
            && !zero_stack::packet::ipv4_fragmentation_allowed(frame.payload())
        {
            return Err(rejected(error(Reason::Protocol)));
        }
        if !request.translate {
            let fragments =
                zero_stack::packet::fragment_packet_lengths(frame.payload(), request.mtu)
                    .map_err(|source| rejected(error(Reason::Protocol).with_source(source)))?;
            let mut lengths = fragments.clone();
            for length in if fragments.is_empty() {
                vec![frame.payload().len()]
            } else {
                fragments
            } {
                lengths.extend(
                    wire.workspace_lengths(length)
                        .map_err(|source| rejected(error(Reason::Capacity).with_source(source)))?,
                );
            }
            let bytes = lengths
                .iter()
                .try_fold(0usize, |sum, n| sum.checked_add(*n))
                .ok_or_else(|| rejected(error(Reason::Capacity)))?;
            frame
                .prepare_transmit_buffers(lengths.len(), bytes)
                .map_err(|source| rejected(error(Reason::Capacity).with_source(source)))?;
            self.returns
                .register_source(
                    source,
                    request.source,
                    request.replies.clone(),
                    request.observer.clone(),
                    zero_stack::packet::packet_conversation_key(frame.payload()),
                )
                .map_err(|source| rejected(error(Reason::Io).with_source(source)))?;
        }
        let hop = zero_stack::packet::ip_hop_limit(frame.payload());
        if !zero_stack::packet::advance_ip_hop(frame.payload_mut()) {
            return Err(rejected(error(Reason::Protocol)));
        }
        let frame = original.take().expect("same original reserved above");
        let association = frame.return_association.clone();
        let ingress = frame.ingress();
        let bytes = frame.payload().len();
        let identification = if frame.payload().first().is_some_and(|byte| byte >> 4 == 4) {
            u32::from(u16::from_be_bytes([frame.payload()[4], frame.payload()[5]]))
        } else {
            0
        };
        let packets = if request.translate {
            match self.returns.translate_owned(
                frame.into_payload(),
                request.local,
                request.replies.clone(),
                request.observer.clone(),
                request.mtu,
                wire,
            ) {
                Ok(packets) => packets,
                Err(failure) => {
                    restore(original, failure.original, association, ingress, hop);
                    return Err(rejected(error(Reason::Io).with_source(failure.source)));
                }
            }
        } else {
            match zero_stack::packet::fragment_ip_packet_prepaid(
                frame.into_payload(),
                request.mtu,
                identification,
            ) {
                Ok(packets) => packets,
                Err(failure) => {
                    restore(original, failure.original, association, ingress, hop);
                    return Err(rejected(error(Reason::Capacity).with_source(failure.cause)));
                }
            }
        };
        permit.send(ForwardedPackets {
            packets,
            observer: request.observer,
            return_channel: request.replies,
        });
        Ok(SendReceipt::accepted(bytes))
    }
}
fn error(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Send, reason, Impact::Packet)
}
fn rejected(error: ChannelError) -> SendFailure {
    SendFailure::new(error, DeliveryState::NotSubmitted)
}

fn restore(
    slot: &mut Option<IpPacket>,
    mut payload: zero_traits::PacketBuffer,
    association: zero_core::packet::ReturnAssociation,
    ingress: Option<zero_core::packet::PacketIngress>,
    hop: Option<u8>,
) {
    if let Some(hop) = hop {
        zero_stack::packet::restore_ip_hop(&mut payload, hop);
    }
    let mut packet =
        IpPacket::new(payload, association).expect("same validated original IP envelope");
    if let Some(ingress) = ingress {
        packet = packet.with_ingress(ingress);
    }
    *slot = Some(packet);
}
