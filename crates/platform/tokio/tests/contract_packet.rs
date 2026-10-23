use crate::contract_support as support;
use std::{
    cell::Cell,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::time::Instant;
use zero_core::{
    component::{EndpointId, ResourceGeneration},
    packet::{IpFamily, IpPacket, ReturnAssociation},
};
use zero_platform_tokio::contract::{
    context::OpenContext,
    inbound::{accept_packet, PacketReply, PacketReplyResult, ReplyOutcome},
    lifecycle::{ChannelControl, CloseReport},
    packet::*,
    *,
};

fn packet() -> IpPacket {
    let mut bytes = vec![0; 20];
    bytes[0] = 0x45;
    bytes[3] = 20;
    IpPacket::new(
        bytes.into(),
        ReturnAssociation {
            endpoint: EndpointId(1),
            generation: ResourceGeneration(2),
            peer: None,
            correlation: 3,
        },
    )
    .unwrap()
}
struct Tx;
impl PacketTx for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        packet: &IpPacket,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        Poll::Ready(Ok(SendReceipt::accepted(packet.payload().len())))
    }
}
struct Rx {
    packet: Option<IpPacket>,
    _single_receiver: Cell<()>,
}
impl PacketRx for Rx {
    fn poll_recv(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Result<Option<IpPacket>, ChannelError>> {
        Poll::Ready(Ok(self.packet.take()))
    }
}
struct Control {
    closed: Arc<AtomicBool>,
    _single_owner: Cell<()>,
}
impl ChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.closed.store(true, Ordering::SeqCst);
    }
    async fn wait_closed(&mut self, _: Instant) -> Result<CloseReport, ChannelError> {
        Ok(CloseReport::default())
    }
}
struct Outbound;
impl PacketOutbound for Outbound {
    type Tx = Tx;
    type Rx = Rx;
    type Control = Control;
    async fn open(
        &self,
        request: PacketRequest,
        context: OpenContext,
    ) -> PacketOpenResult<Tx, Rx, Control> {
        assert!(context.identity.session().is_none());
        assert_eq!(context.packet_facts()?.endpoint, request.endpoint);
        Ok(PacketSession {
            tx: Tx,
            rx: Rx {
                packet: Some(packet()),
                _single_receiver: Cell::new(()),
            },
            control: Control {
                closed: Arc::new(AtomicBool::new(false)),
                _single_owner: Cell::new(()),
            },
            limits: request.limits,
        })
    }
}

#[tokio::test]
async fn dynamic_packet_session_preserves_ip_envelope_correlation_and_mtu() {
    let outbound = erase_packet(Outbound);
    let mut session = outbound
        .open(
            PacketRequest {
                endpoint: EndpointId(1),
                limits: PacketLimits {
                    mtu: 20,
                    families: vec![IpFamily::Ipv4],
                    source_adaptation: SourceAdaptation::Preserved,
                    return_association: ReturnGuarantee::Flow,
                },
            },
            support::packet_context(),
        )
        .await
        .unwrap();
    let frame = recv(session.rx.as_mut()).await.unwrap().unwrap();
    assert_eq!(frame.return_association.correlation, 3);
    assert_eq!(frame.return_association.generation, ResourceGeneration(2));
    assert_eq!(
        send(
            session.tx.as_mut(),
            session.control.as_mut(),
            &frame,
            &session.limits
        )
        .await
        .unwrap(),
        SendReceipt::accepted(20)
    );
    assert!(recv(session.rx.as_mut()).await.unwrap().is_none());
    session.limits.mtu = 19;
    let error = send(
        session.tx.as_mut(),
        session.control.as_mut(),
        &frame,
        &session.limits,
    )
    .await
    .unwrap_err();
    assert_eq!(error.error.reason, Reason::Capacity);
    assert_eq!(error.error.impact, Impact::Packet);
    assert_eq!(error.delivery, DeliveryState::NotSubmitted);
    session.limits.mtu = 20;
    session.limits.families = vec![IpFamily::Ipv6];
    assert_eq!(
        send(
            session.tx.as_mut(),
            session.control.as_mut(),
            &frame,
            &session.limits
        )
        .await
        .unwrap_err()
        .error
        .reason,
        Reason::Protocol
    );
}

struct Reply(zero_core::packet::PacketFacts);
impl PacketReply for Reply {
    type Tx = Tx;
    type Rx = Rx;
    type Control = Control;
    fn facts(&self) -> &zero_core::packet::PacketFacts {
        &self.0
    }
    async fn respond(self, outcome: ReplyOutcome) -> PacketReplyResult<Tx, Rx, Control> {
        if outcome != ReplyOutcome::LocalChannelReady {
            return Ok(None);
        }
        Ok(Some(PacketSession {
            tx: Tx,
            rx: Rx {
                packet: Some(packet()),
                _single_receiver: Cell::new(()),
            },
            control: Control {
                closed: Arc::new(AtomicBool::new(false)),
                _single_owner: Cell::new(()),
            },
            limits: PacketLimits {
                mtu: 20,
                families: vec![IpFamily::Ipv4],
                source_adaptation: SourceAdaptation::Preserved,
                return_association: ReturnGuarantee::Flow,
            },
        }))
    }
}

#[tokio::test]
async fn packet_ingress_handoff_has_endpoint_facts_without_a_business_session() {
    let context = support::packet_context();
    assert_eq!(context.identity.session(), None);
    let accepted = accept_packet(Reply(context.packet_facts().unwrap().clone()));
    assert_eq!(accepted.facts().endpoint, EndpointId(1));
    let mut session = accepted
        .respond(ReplyOutcome::LocalChannelReady)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        recv(session.rx.as_mut())
            .await
            .unwrap()
            .unwrap()
            .return_association
            .correlation,
        3
    );
}

#[tokio::test]
async fn wrong_identity_plane_is_rejected_before_the_packet_backend_is_called() {
    let outbound = erase_packet(Outbound);
    let error = outbound
        .open(
            PacketRequest {
                endpoint: EndpointId(1),
                limits: PacketLimits {
                    mtu: 20,
                    families: vec![IpFamily::Ipv4],
                    source_adaptation: SourceAdaptation::Preserved,
                    return_association: ReturnGuarantee::Flow,
                },
            },
            support::context(),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.stage, Stage::Prepare);
    assert_eq!(error.delivery, DeliveryState::NotSubmitted);
}
