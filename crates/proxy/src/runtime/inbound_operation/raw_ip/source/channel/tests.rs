//! Actual bounded channel and its original consumed caller join. No native proof.
use super::*;
use std::{future::Future, pin::Pin, task::Poll, time::Duration};
use zero_core::{
    component::*,
    packet::{PacketIngress, ReturnAssociation},
};
use zero_platform_tokio::contract::{
    packet,
    scope::{TaskClass, TaskScope},
};
fn facts() -> PacketFacts {
    PacketFacts {
        endpoint: EndpointId(9),
        inbound: RoleInstanceId {
            component: ComponentId(9),
            role: RoleId::Inbound(InboundId(9)),
            generation: ResourceGeneration(2),
        },
        carrier_source: None,
        subject: None,
        peer: None,
    }
}
fn frame(port: u16) -> IpPacket {
    let facts = facts();
    IpPacket::new(
        zero_stack::packet::build_udp(
            "10.0.0.2".parse().unwrap(),
            "192.0.2.1".parse().unwrap(),
            port,
            443,
            b"owned-source",
        )
        .into(),
        ReturnAssociation {
            endpoint: facts.endpoint,
            generation: facts.inbound.generation,
            peer: None,
            correlation: 17,
        },
    )
    .unwrap()
    .with_ingress(PacketIngress {
        local_destination: false,
        mtu: 1420,
    })
}
fn tx(
    sender: mpsc::Sender<IpPacket>,
    cancel: zero_platform_tokio::contract::CancellationToken,
    pending: Arc<Mutex<Option<IpPacket>>>,
) -> Tx {
    Tx {
        sender: PollSender::new(sender),
        cancel,
        pending,
        facts: facts(),
    }
}
#[tokio::test]
async fn owned_channel_handoff_keeps_the_original_buffer_and_return_facts() {
    let (send, mut received) = mpsc::channel(1);
    let pending = Arc::new(Mutex::new(None));
    let mut sender = tx(
        send,
        zero_platform_tokio::contract::CancellationToken::new(),
        pending.clone(),
    );
    let original = frame(40000);
    let pointer = original.payload().as_ptr();
    let association = original.return_association.clone();
    let ingress = original.ingress();
    let mut original = Some(original);
    let receipt =
        std::future::poll_fn(|cx| Pin::new(&mut sender).poll_send_owned(cx, &mut original))
            .await
            .unwrap();
    assert_eq!(receipt.delivery, DeliveryState::LocallyAccepted);
    assert!(original.is_none());
    assert!(pending.lock().unwrap().is_none());
    let actual = received.recv().await.unwrap();
    assert_eq!(actual.payload().as_ptr(), pointer);
    assert_eq!(actual.return_association, association);
    assert_eq!(actual.ingress(), ingress);
}
#[tokio::test]
async fn closed_original_channel_returns_not_submitted_same_packet_and_metadata() {
    let (send, received) = mpsc::channel(1);
    drop(received);
    let pending = Arc::new(Mutex::new(None));
    let mut sender = tx(
        send,
        zero_platform_tokio::contract::CancellationToken::new(),
        pending.clone(),
    );
    let packet = frame(40001);
    let pointer = packet.payload().as_ptr();
    let association = packet.return_association.clone();
    let ingress = packet.ingress();
    let mut packet = Some(packet);
    let failure = std::future::poll_fn(|cx| Pin::new(&mut sender).poll_send_owned(cx, &mut packet))
        .await
        .unwrap_err();
    assert_eq!(failure.delivery, DeliveryState::NotSubmitted);
    let returned = packet.unwrap();
    assert_eq!(returned.payload().as_ptr(), pointer);
    assert_eq!(returned.return_association, association);
    assert_eq!(returned.ingress(), ingress);
    assert!(pending.lock().unwrap().is_none());
}
#[tokio::test]
async fn cancelled_owned_capacity_wait_keeps_the_source_original_until_same_real_join() {
    let (send, mut received) = mpsc::channel(1);
    send.send(frame(40002)).await.unwrap();
    let pending = Arc::new(Mutex::new(None));
    let cancel = zero_platform_tokio::contract::CancellationToken::new();
    let mut scope = TaskScope::suspended_hosted(
        tokio::runtime::Handle::current(),
        std::num::NonZeroUsize::new(1),
    );
    scope.enable_registration().unwrap();
    let stopped = cancel.clone();
    // This original consumer owns/drains this same channel; it is not an
    // unrelated marker task or a native-close provider fixture.
    let (_,witness)=scope.registrar().spawn_tracked(TaskClass::Critical,async move {loop {tokio::select!{biased;_=stopped.cancelled()=>break,next=received.recv()=>if next.is_none(){break;}}}Ok(())}).unwrap();
    let (stop, _) = watch::channel(false);
    let mut control = Control {
        cancel: cancel.clone(),
        stop,
        witness: witness.clone(),
        pending: pending.clone(),
    };
    let mut sender = tx(send, cancel, pending.clone());
    let original = frame(40003);
    let pointer = original.payload().as_ptr();
    let limits = PacketLimits {
        mtu: 1500,
        families: vec![zero_core::packet::IpFamily::Ipv4],
        source_adaptation: SourceAdaptation::Preserved,
        return_association: ReturnGuarantee::Endpoint,
    };
    let mut writing = Box::pin(packet::send_owned(
        Pin::new(&mut sender),
        &mut control,
        original,
        &limits,
    ));
    std::future::poll_fn(|cx| {
        assert!(writing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(writing);
    assert_eq!(
        pending.lock().unwrap().as_ref().unwrap().payload().as_ptr(),
        pointer
    );
    assert!(!witness.is_joined());
    let mut closing =
        Box::pin(control.wait_closed(tokio::time::Instant::now() + Duration::from_secs(2)));
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(closing);
    assert!(pending.lock().unwrap().is_some());
    let receipt = tokio::time::timeout(Duration::from_secs(2), scope.next_receipt())
        .await
        .unwrap()
        .unwrap();
    assert!(witness.matches_joined(receipt.joined()));
    scope.acknowledge(&receipt).unwrap();
    let closed = control
        .wait_closed(tokio::time::Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(closed.is_complete());
    assert!(closed.closed_io.is_empty());
    assert!(pending.lock().unwrap().is_none());
    scope.begin_shutdown();
    assert!(scope.next_receipt().await.is_none());
    assert!(scope.is_drained());
}
