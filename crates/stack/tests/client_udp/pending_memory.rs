//! A real sender's retained payload is a separate allocation from its IP frame.
use super::*;
use zero_tun::packet_memory::{PacketMemoryLimit, PacketReturnTotals};
type ActualSender = (
    Arc<PacketReturnTotals>,
    zero_tun::packet_memory::NativeReadGrant,
    ClientUdpStack,
    ClientUdpSocket,
    mpsc::Receiver<zero_traits::PacketBuffer>,
    Endpoint,
);
fn actual_sender(bytes: usize, frames: usize) -> ActualSender {
    let limit = PacketMemoryLimit {
        frames: 4,
        bytes: 128,
    };
    let root = Arc::new(PacketReturnTotals::new_with_transmit(
        limit,
        limit,
        PacketMemoryLimit { frames, bytes },
    ));
    let grant = root.native_grant(64).unwrap();
    let (send, receive) = mpsc::channel::<zero_traits::PacketBuffer>(1);
    let output = crate::packet_output::PacketSender::from(send)
        .with_packet_memory_view(
            grant.memory_view().unwrap(),
            crate::packet_output::memory::PacketWireMemory::Native,
        )
        .unwrap();
    let local = "10.0.0.1".parse().unwrap();
    let stack = ClientUdpStack::new_with_queue(vec![local], output, 1500, None, Default::default())
        .unwrap();
    let socket = stack.bind(local).unwrap();
    let target = Endpoint {
        ip: "10.0.0.2".parse().unwrap(),
        port: 53,
    };
    (root, grant, stack, socket, receive, target)
}
#[tokio::test]
async fn pending_payload_has_its_own_actual_global_credit_until_all_bytes_return() {
    let (root, grant, _stack, mut socket, _receive, _) = actual_sender(4, 1);
    let sender = socket.sender();
    let original = sender.copy_pending_payload(b"four").unwrap();
    assert_eq!(original.allocation_bytes(), 4);
    assert_eq!(
        root.transmit().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 4
        }
    );
    assert!(matches!(
        sender.copy_pending_payload(b"next"),
        Err(zero_traits::PacketDeriveError::Busy)
    ));
    socket.close();
    grant.revoke();
    assert_eq!(&original[..], b"four");
    assert_eq!(
        root.transmit().occupied.bytes,
        4,
        "logical/native stop cannot refund retained payload memory"
    );
    assert!(matches!(
        sender.copy_pending_payload(b"next"),
        Err(zero_traits::PacketDeriveError::Revoked)
    ));
    drop(original);
    assert_eq!(
        root.transmit().occupied,
        PacketMemoryLimit {
            frames: 0,
            bytes: 0
        }
    );
}
#[tokio::test]
async fn actual_udp_envelope_and_pending_payload_remain_separately_charged() {
    let (root, grant, _stack, socket, mut receive, target) = actual_sender(36, 2);
    let sender = socket.sender();
    let original = sender.copy_pending_payload(b"four").unwrap();
    sender.send_to(&original, target).await.unwrap();
    let wire = receive.recv().await.unwrap();
    assert_eq!(packet::parse_udp(&wire).unwrap().payload, b"four");
    assert_eq!(
        root.transmit().occupied,
        PacketMemoryLimit {
            frames: 2,
            bytes: 36
        }
    );
    drop(original);
    grant.revoke();
    assert_eq!(
        root.transmit().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 32
        }
    );
    drop(wire);
    assert_eq!(
        root.transmit().occupied,
        PacketMemoryLimit {
            frames: 0,
            bytes: 0
        }
    );
}
