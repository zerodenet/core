//! Actual checked-out Native1 storage, not a bare library input slice.
use super::*;
use crate::packet_output::{memory::PacketWireMemory, PacketSender};
use zero_tun::packet_memory::{
    NativeReadGrant, PacketMemoryError, PacketMemoryLimit, PacketReturnTotals,
};
fn input(
    read: &NativeReadGrant,
    remote: Endpoint,
    local: Endpoint,
    payload: &[u8],
) -> zero_traits::PacketBuffer {
    let bytes = packet::build_udp(remote.ip, local.ip, remote.port, local.port, payload);
    let mut original = read.try_buffer().unwrap();
    original.as_mut_bytes()[..bytes.len()].copy_from_slice(&bytes);
    original.into_packet(bytes.len()).unwrap()
}
fn fixture() -> (
    Arc<PacketReturnTotals>,
    NativeReadGrant,
    ClientUdpStack,
    ClientUdpSocket,
    Endpoint,
    mpsc::Receiver<zero_traits::PacketBuffer>,
) {
    let root = Arc::new(PacketReturnTotals::new(
        PacketMemoryLimit {
            frames: 1,
            bytes: 1,
        },
        PacketMemoryLimit {
            frames: 1,
            bytes: 64,
        },
    ));
    let read = root.native_grant(64).unwrap();
    let (sender, output) = mpsc::channel::<zero_traits::PacketBuffer>(1);
    let sender = PacketSender::from(sender)
        .with_packet_memory_view(read.memory_view().unwrap(), PacketWireMemory::Native)
        .unwrap();
    let local = "10.0.0.2".parse().unwrap();
    let stack = ClientUdpStack::new_with_queue(
        vec![local],
        sender,
        1500,
        None,
        ClientUdpReceiveQueueConfig {
            capacity: 1,
            max_payload_bytes: Some(65_527),
            max_stack_payload_bytes: Some(65_527),
        },
    )
    .unwrap();
    let socket = stack.bind(local).unwrap();
    let remote = Endpoint {
        ip: "203.0.113.9".parse().unwrap(),
        port: 53,
    };
    (root, read, stack, socket, remote, output)
}
#[tokio::test]
async fn one_actual_read_moves_through_client_udp_and_stays_charged_after_queue_byte_release_and_logical_close(
) {
    let (root, read, stack, mut socket, remote, mut output) = fixture();
    socket.send_to(b"query", remote).await.unwrap();
    drop(output.recv().await.unwrap());
    let baseline = root.driver_metadata().occupied;
    let packet = input(&read, remote, socket.local_endpoint(), b"same");
    let address = packet[28..].as_ptr();
    stack.feed_correlated_packet(packet).unwrap();
    let event = std::future::poll_fn(|cx| socket.poll_recv_packet_event(cx))
        .await
        .unwrap();
    let ClientUdpPacketEvent::Datagram(packet) = event else {
        panic!("expected original UDP payload");
    };
    assert_eq!(packet.source, remote);
    assert_eq!(packet.payload.as_ptr(), address);
    assert_eq!(packet.payload.as_ref(), b"same");
    assert_eq!(packet.payload.allocation_bytes(), 64);
    assert_eq!(stack.inner.receive_budget.as_ref().unwrap().test_used(), 0);
    assert!(matches!(read.try_buffer(), Err(PacketMemoryError::Busy)));
    let closed = socket.close();
    assert!(closed.io_quiescent());
    assert!(root.driver_metadata().occupied.bytes() > baseline.bytes());
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 64
        }
    );
    assert!(matches!(read.try_buffer(), Err(PacketMemoryError::Busy)));
    drop(packet);
    assert_eq!(root.driver_metadata().occupied, baseline);
    assert!(read.try_buffer().is_ok());
    read.revoke();
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 0,
            bytes: 0
        }
    );
}
#[tokio::test]
async fn owned_reply_metadata_refusal_keeps_old_policy_and_returns_the_same_original_read_without_an_unpaid_queue_row(
) {
    let (root, read, stack, mut socket, remote, mut output) = fixture();
    socket.send_to(b"query", remote).await.unwrap();
    drop(output.recv().await.unwrap());
    let baseline = root.driver_metadata();
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        baseline.occupied,
    )
    .unwrap();
    let packet = input(&read, remote, socket.local_endpoint(), b"new");
    assert!(
        stack.feed_correlated_packet(packet).is_ok(),
        "bounded refusal consumes the matched reply, never admits it as new traffic"
    );
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(socket.poll_recv_packet_event(&mut cx).is_pending());
    assert!(read.try_buffer().is_ok());
    assert_eq!(root.driver_metadata().occupied, baseline.occupied);
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        baseline.limit,
    )
    .unwrap();
    let original = input(&read, remote, socket.local_endpoint(), b"kept");
    stack.feed_correlated_packet(original).unwrap();
    let event = std::future::poll_fn(|cx| socket.poll_recv_packet_event(cx))
        .await
        .unwrap();
    let ClientUdpPacketEvent::Datagram(packet) = event else {
        panic!("expected actual payload");
    };
    assert_eq!(packet.payload.as_ref(), b"kept");
    drop(packet);
    assert!(read.try_buffer().is_ok());
    assert!(socket.close().io_quiescent());
}
#[tokio::test]
async fn unmatched_owned_reply_returns_the_identical_original_for_the_existing_source_route() {
    let (root, read, stack, socket, remote, _output) = fixture();
    let baseline = root.driver_metadata().occupied;
    let original = input(&read, remote, socket.local_endpoint(), b"miss");
    let pointer = original.as_ptr();
    let returned = stack.feed_correlated_packet(original).unwrap_err();
    assert_eq!(returned.as_ptr(), pointer);
    assert_eq!(packet::parse_udp(&returned).unwrap().payload, b"miss");
    assert_eq!(root.driver_metadata().occupied, baseline);
    assert!(matches!(read.try_buffer(), Err(PacketMemoryError::Busy)));
    drop(returned);
    assert!(read.try_buffer().is_ok());
}
