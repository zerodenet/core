//! Failed input retirement drains real queued datagrams; live reads stay open.
use super::super::*;

fn fixture() -> (
    mpsc::Sender<TunDatagram>,
    TunUdpResponder,
    Arc<UserUdpStack>,
) {
    use zero_tun::packet_memory::{PacketMemoryLimit, PacketReturnTotals};
    let root = Arc::new(PacketReturnTotals::new(
        PacketMemoryLimit {
            frames: 1,
            bytes: 64,
        },
        PacketMemoryLimit {
            frames: 1,
            bytes: 64,
        },
    ));
    let (sender, receiver) = mpsc::channel(4);
    let responder = TunUdpRelay {
        source: SocketAddress::new(IpAddress::V4([10, 0, 0, 2]), 12000),
        memory: root.driver_metadata_view(),
        receiver,
    }
    .into_datagram_udp_parts()
    .0;
    let (output, _packets) = mpsc::channel(1);
    let (_, stack) = zero_stack::UserNetworkStack::new(output, 1440).into_parts();
    (sender, responder, stack)
}
fn packet(value: u8) -> TunDatagram {
    TunDatagram {
        peer_identity: None,
        destination: SocketAddress::new(IpAddress::V4([203, 0, 113, 9]), 5355),
        payload: vec![value].into(),
    }
}

#[tokio::test]
async fn failed_first_tun_dispatch_drains_original_queue_before_returning() {
    let (sender, mut responder, stack) = fixture();
    for value in 1..=3 {
        sender.send(packet(value)).await.unwrap();
    }
    let first = responder
        .read_inbound_dispatch(&stack)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.payload(), &[1]);
    // The actual dispatch failure path does not publish a response mapping.
    assert!(responder.session_destinations.is_empty());
    for value in 2..=3 {
        let queued = responder
            .read_inbound_dispatch(&stack)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(queued.payload(), &[value]);
        assert!(
            sender.is_closed(),
            "no new input enters the retiring association"
        );
    }
    assert!(responder
        .read_inbound_dispatch(&stack)
        .await
        .unwrap()
        .is_none());
    assert!(sender.send(packet(4)).await.is_err());
}

#[tokio::test]
async fn successful_tun_dispatch_retains_the_actual_pending_association_read() {
    let (sender, mut responder, stack) = fixture();
    sender.send(packet(1)).await.unwrap();
    let first = responder
        .read_inbound_dispatch(&stack)
        .await
        .unwrap()
        .unwrap();
    responder.on_dispatch_success(7, &first);
    let next = responder.read_inbound_dispatch(&stack);
    tokio::pin!(next);
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(next.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    assert!(!sender.is_closed());
    sender.send(packet(2)).await.unwrap();
    assert_eq!(next.await.unwrap().unwrap().payload(), &[2]);
}
