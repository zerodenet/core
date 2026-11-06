use super::*;
use std::task::{Context, Poll};
use zero_core::packet::IpPacket;
use zero_platform_tokio::contract::{packet::PacketTx, DeliveryState, SendFailure, SendReceipt};
pub(super) struct Tx(pub(super) Arc<Shared>);
impl PacketTx for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &IpPacket,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        // The production route moves its original. A borrowed-copy fallback
        // would conceal cancellation custody and is deliberately unavailable.
        Poll::Ready(Err(rejected(error(Stage::Send, Reason::Unsupported))))
    }
    fn poll_send_owned(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        original: &mut Option<IpPacket>,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        Poll::Ready(self.0.submit(original))
    }
}
impl Shared {
    fn submit(&self, original: &mut Option<IpPacket>) -> Result<SendReceipt, SendFailure> {
        if self.cancel.is_cancelled() {
            return Err(rejected(error(Stage::Send, Reason::Revoked)));
        }
        self.carrier.origin().map_err(rejected)?;
        let frame = original
            .as_ref()
            .ok_or_else(|| rejected(error(Stage::Send, Reason::Protocol)))?;
        if frame.return_association.endpoint != self.facts.endpoint
            || frame.return_association.generation != self.facts.inbound.generation
        {
            return Err(rejected(error(Stage::Send, Reason::StalePlan)));
        }
        let source = zero_stack::packet::ip_source(frame.payload())
            .ok_or_else(|| rejected(error(Stage::Send, Reason::Protocol)))?;
        let destination = zero_stack::packet::ip_destination(frame.payload())
            .ok_or_else(|| rejected(error(Stage::Send, Reason::Protocol)))?;
        use crate::runtime::raw_ip::RawIpOutboundPlan;
        let plan = &self.carrier.options.plan;
        if !self.translate && plan.is_local_address(source) {
            return Err(rejected(error(Stage::Send, Reason::InvalidConfig)));
        }
        let peer = plan
            .peer_for_target(destination)
            .map_err(|source| rejected(failure(source)))?;
        let (device, local) = self.carrier.device(destination).map_err(rejected)?;
        let mtu = usize::from(plan.mtu());
        let (sink, cookie) = self.sink(peer.peer_index, frame).map_err(rejected)?;
        let response = if frame.payload().len() > mtu
            && !zero_stack::packet::ipv4_fragmentation_allowed(frame.payload())
        {
            Some(zero_stack::packet::build_icmp_response(
                frame.payload(),
                mtu,
            ))
        } else if zero_stack::packet::ip_hop_limit(frame.payload()).is_some_and(|hop| hop <= 1) {
            Some(zero_stack::packet::build_icmp_time_exceeded_response(
                frame.payload(),
                local,
                mtu,
            ))
        } else {
            None
        };
        if let Some(response) = response {
            if let Some(response) = response {
                sink.try_send_buffer(response.into()).map_err(|source| {
                    rejected(error(Stage::Send, Reason::Capacity).with_source(source))
                })?;
            }
            let bytes = original
                .take()
                .expect("validated same original")
                .payload()
                .len();
            return Ok(SendReceipt::accepted(bytes));
        }
        let authenticated_peer = frame.return_association.peer.clone();
        let observation = self.observation(cookie);
        device.try_submit_standard_packet(
            &self.carrier.scope,
            original,
            crate::runtime::raw_ip::PacketSubmission {
                source: crate::runtime::raw_ip::ReturnSource::Endpoint {
                    endpoint: self.facts.endpoint,
                    generation: self.facts.inbound.generation,
                    peer: authenticated_peer,
                },
                replies: sink,
                observer: Some(observation),
                local,
                translate: self.translate,
                mtu,
            },
        )
    }
}
fn rejected(error: ChannelError) -> SendFailure {
    SendFailure::new(error, DeliveryState::NotSubmitted)
}
