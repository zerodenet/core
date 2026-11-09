use super::*;
use crate::packet_output::{memory::PacketWireMemory, PacketSender};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Weak,
    },
    task::{Context, Poll, Wake, Waker},
};
use zero_tun::packet_memory::{PacketMemoryLimit, PacketReturnTotals};
fn input(data: &[u8]) -> Vec<u8> {
    packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "203.0.113.8".parse().unwrap(),
        12000,
        53,
        data,
    )
}
#[tokio::test]
async fn queued_payload_capacity_refusal_preserves_original_and_dequeue_releases_only_empty_table()
{
    let root = Arc::new(PacketReturnTotals::new(
        PacketMemoryLimit {
            frames: 1,
            bytes: 1,
        },
        PacketMemoryLimit {
            frames: 2,
            bytes: 68,
        },
    ));
    let original = root.native_grant(64).unwrap();
    let view = original.memory_view().unwrap();
    let metadata = root.driver_metadata().occupied;
    let (send, _outgoing) = mpsc::channel::<zero_traits::PacketBuffer>(1);
    let sender = PacketSender::from(send)
        .with_packet_memory_view(view, PacketWireMemory::Native)
        .unwrap();
    let stack = UserUdpStack::new_output(sender, 1500);
    stack
        .feed_with_peer(&input(b"kept"), Some(Arc::from("peer-a")))
        .await;
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 2,
            bytes: 68
        }
    );
    stack.feed(&input(b"rejected")).await;
    assert!(!stack.quiescent().await);
    let datagram = stack.recv_datagram_with_peer().await.unwrap();
    assert_eq!(datagram.data.as_ref(), b"kept");
    assert_eq!(datagram.peer_identity.as_deref(), Some("peer-a"));
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 2,
            bytes: 68
        }
    );
    assert!(stack.quiescent().await);
    assert!(root.driver_metadata().occupied.bytes() > metadata.bytes());
    // Dequeue transfers the same owner; freeing the empty queue table cannot
    // refund the payload that is still held by its actual next consumer.
    drop(datagram);
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 64
        }
    );
    assert_eq!(root.driver_metadata().occupied, metadata);
}
struct Reentrant {
    stack: Weak<UserUdpStack>,
    observed: AtomicUsize,
}
impl Wake for Reentrant {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let stack = self.stack.upgrade().unwrap();
        assert!(
            stack.datagrams.try_lock().is_ok(),
            "actual refund/wake while UDP queue is locked"
        );
        self.observed.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn exact_table_and_payload_refunds_notify_outside_the_same_receive_queue_lock() {
    let root = Arc::new(PacketReturnTotals::default());
    let original = root.native_grant(64).unwrap();
    let view = original.memory_view().unwrap();
    let (send, _outgoing) = mpsc::channel::<zero_traits::PacketBuffer>(1);
    let stack = Arc::new(UserUdpStack::new_output(
        PacketSender::from(send)
            .with_packet_memory_view(view.clone(), PacketWireMemory::Native)
            .unwrap(),
        1500,
    ));
    stack.feed(&input(b"one")).await;
    let observe = Arc::new(Reentrant {
        stack: Arc::downgrade(&stack),
        observed: AtomicUsize::new(0),
    });
    let waker = Waker::from(observe.clone());
    let mut cx = Context::from_waker(&waker);
    let changed = view.changed();
    tokio::pin!(changed);
    assert!(matches!(changed.as_mut().poll(&mut cx), Poll::Pending));
    let mut bytes = [0; 3];
    let (size, _, _, _) = stack.recv_from_with_peer(&mut bytes).await.unwrap();
    assert_eq!(&bytes[..size], b"one");
    assert!(observe.observed.load(Ordering::SeqCst) > 0);
    assert_eq!(root.native().occupied.frames, 1);
}
#[tokio::test]
async fn owned_udp_return_keeps_fragment_and_wire_credit_until_the_actual_output_is_returned() {
    let root = Arc::new(PacketReturnTotals::default());
    let original = root.native_grant(64).unwrap();
    let view = original.memory_view().unwrap();
    let (send, mut output) = mpsc::channel::<zero_traits::PacketBuffer>(4);
    let stack = UserUdpStack::new_output(
        PacketSender::from(send)
            .with_packet_memory_view(
                view,
                PacketWireMemory::PaddedEnvelope {
                    multiple: 16,
                    overhead: 32,
                },
            )
            .unwrap(),
        100,
    );
    let source = SocketAddress::new(zero_traits::IpAddress::V4([203, 0, 113, 8]), 53);
    let destination = SocketAddress::new(zero_traits::IpAddress::V4([10, 0, 0, 2]), 12000);
    stack.send_to(&[7; 180], source, destination).await;
    assert!(root.transmit().occupied.frames > 0);
    let mut fragments = Vec::new();
    while let Ok(packet) = output.try_recv() {
        assert!(packet.len() <= 100);
        fragments.push(packet);
    }
    assert_eq!(fragments.len(), 3);
    assert!(fragments
        .iter()
        .all(|packet| packet.allocation_bytes() == packet.len()));
    drop(fragments);
    assert_eq!(root.transmit().occupied.frames, 0);
}

#[path = "tests/returns.rs"]
mod returns;
