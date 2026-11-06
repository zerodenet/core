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
        let hop = zero_stack::packet::ip_hop_limit(frame.payload());
        if !zero_stack::packet::advance_ip_hop(frame.payload_mut()) {
            return Err(rejected(error(Reason::Protocol)));
        }
        let result = (|| {
            let packets = if request.translate {
                let translated = self
                    .returns
                    .translate(
                        frame.payload(),
                        request.local,
                        request.replies.clone(),
                        request.observer.clone(),
                    )
                    .map_err(|source| rejected(error(Reason::Io).with_source(source)))?;
                if translated.len() > request.mtu {
                    let fragments =
                        zero_stack::packet::fragment_forwarded_packet(&translated, request.mtu);
                    if fragments.is_empty() {
                        return Err(rejected(error(Reason::Protocol)));
                    }
                    fragments.into_iter().map(Into::into).collect()
                } else {
                    vec![translated.into()]
                }
            } else {
                self.returns
                    .register_source(
                        source,
                        request.source,
                        request.replies.clone(),
                        request.observer.clone(),
                        zero_stack::packet::packet_conversation_key(frame.payload()),
                    )
                    .map_err(|source| rejected(error(Reason::Io).with_source(source)))?;
                if frame.payload().len() > request.mtu {
                    let fragments =
                        zero_stack::packet::fragment_forwarded_packet(frame.payload(), request.mtu);
                    if fragments.is_empty() {
                        return Err(rejected(error(Reason::Protocol)));
                    }
                    fragments.into_iter().map(Into::into).collect()
                } else {
                    Vec::new()
                }
            };
            Ok(packets)
        })();
        let mut packets = match result {
            Ok(packets) => packets,
            Err(source) => {
                if let Some(hop) = hop {
                    zero_stack::packet::restore_ip_hop(frame.payload_mut(), hop);
                }
                return Err(source);
            }
        };
        let frame = original.take().expect("same original reserved above");
        let bytes = frame.payload().len();
        if !request.translate && packets.is_empty() {
            // Move the original allocation, never copy a borrowed packet to
            // claim owned submission. Fragment/translation outputs are actual
            // stack transformations and consume this same original here.
            packets.push(frame.into_payload());
        }
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
