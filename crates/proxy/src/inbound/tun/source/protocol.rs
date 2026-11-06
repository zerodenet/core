//! Authentication-free Packet handoff bound to this actual host endpoint.
use super::*;
use zero_platform_tokio::contract::context::{AcceptContext, HandshakeContext};
impl InboundSource for Source {
    type Handshake = PendingInbound;
    async fn next(
        &mut self,
        context: AcceptContext,
    ) -> Result<Option<PendingInbound>, ChannelError> {
        if self.shared.cancel.is_cancelled() || context.cancel.is_cancelled() {
            return Ok(None);
        }
        if self.shared.state.lock().unwrap().source_taken {
            tokio::select! { _ = self.shared.cancel.cancelled() => {}, _ = context.cancel.cancelled() => {} }
            return Ok(None);
        }
        let original = self
            .shared
            .metadata
            .identity()
            .ok_or_else(|| error(Stage::Receive, Reason::Revoked))?;
        self.shared.state.lock().unwrap().source_taken = true;
        let facts = PacketFacts {
            endpoint: EndpointId(self.instance.component.0),
            inbound: RoleInstanceId {
                component: self.instance.component,
                generation: self.instance.generation,
                role: RoleId::Inbound(self.inbound),
            },
            carrier_source: None,
            subject: None,
            peer: None,
        };
        Ok(Some(PendingInbound::from_packet_device(
            Handshake {
                shared: self.shared.clone(),
                facts,
            },
            original,
        )))
    }
}
struct Handshake {
    shared: Arc<Shared>,
    facts: PacketFacts,
}
impl InboundHandshake for Handshake {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        if context.inbound != self.facts.inbound || context.accepted.is_some() {
            return Err(error(Stage::Handshake, Reason::StalePlan));
        }
        Ok(AcceptedRequest::Packet(accept_packet(Reply {
            shared: self.shared,
            facts: self.facts,
        })))
    }
}
struct Reply {
    shared: Arc<Shared>,
    facts: PacketFacts,
}
impl PacketReply for Reply {
    type Tx = channel::Tx;
    type Rx = channel::Rx;
    type Control = channel::Control;
    fn link_mtu(&self) -> Option<u16> {
        Some(self.shared.mtu)
    }
    fn facts(&self) -> &PacketFacts {
        &self.facts
    }
    async fn respond(
        self,
        _: ReplyOutcome,
    ) -> PacketReplyResult<Self::Tx, Self::Rx, Self::Control> {
        Err(error(Stage::Handshake, Reason::Unsupported))
    }
    async fn respond_observed(
        self,
        outcome: ReplyOutcome,
        observer: Arc<dyn PacketObserver>,
    ) -> PacketReplyResult<Self::Tx, Self::Rx, Self::Control> {
        if outcome != ReplyOutcome::LocalChannelReady {
            self.shared.cancel.cancel();
            return Ok(None);
        }
        let witness = self
            .shared
            .metadata
            .driver()
            .ok_or_else(|| error(Stage::Handshake, Reason::Revoked))?;
        let (incoming, received) = mpsc::channel(256);
        let (responses, outgoing) = mpsc::channel(256);
        let session = PacketSession {
            tx: channel::Tx {
                sender: zero_platform_tokio::PollSender::new(responses),
                shared: self.shared.clone(),
                facts: self.facts.clone(),
            },
            rx: channel::Rx(received),
            control: channel::Control {
                shared: self.shared.clone(),
                driver: witness,
            },
            limits: PacketLimits {
                mtu: 65_535,
                families: vec![
                    zero_core::packet::IpFamily::Ipv4,
                    zero_core::packet::IpFamily::Ipv6,
                ],
                source_adaptation: SourceAdaptation::Preserved,
                return_association: ReturnGuarantee::Endpoint,
            },
        };
        let mut state = self.shared.state.lock().unwrap();
        if state.start.is_some() {
            return Err(error(Stage::Handshake, Reason::StalePlan));
        }
        state.start = Some(Start {
            facts: self.facts,
            observer,
            incoming,
            outgoing,
        });
        drop(state);
        self.shared.changed.notify_waiters();
        Ok(Some(session))
    }
}
