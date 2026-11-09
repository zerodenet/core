//! A real original read is still charged when Kernel UDP accepts its range.
use super::*;
use crate::packet_output::{memory::PacketWireMemory, PacketSender};
use zero_tun::packet_memory::{PacketMemoryError, PacketMemoryLimit, PacketReturnTotals};
#[tokio::test]
async fn one_actual_native_read_moves_into_udp_range_without_a_second_frame_or_copy() {
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
    let view = read.memory_view().unwrap();
    let baseline = root.driver_metadata().occupied;
    let input = packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "203.0.113.8".parse().unwrap(),
        12000,
        53,
        b"same",
    );
    let mut incoming = read.try_buffer().unwrap();
    incoming.as_mut_bytes()[..input.len()].copy_from_slice(&input);
    let original = incoming.into_packet(input.len()).unwrap();
    let payload_address = original[28..].as_ptr();
    assert!(matches!(read.try_buffer(), Err(PacketMemoryError::Busy)));
    let (sender, _outgoing) = mpsc::channel::<zero_traits::PacketBuffer>(1);
    let stack = UserUdpStack::new_output(
        PacketSender::from(sender)
            .with_packet_memory_view(view, PacketWireMemory::Native)
            .unwrap(),
        1500,
    );
    stack
        .feed_owned_with_peer(original, Some(Arc::from("actual-peer")))
        .await;
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 64
        }
    );
    let packet = stack.recv_datagram_with_peer().await.unwrap();
    assert_eq!(packet.data.as_ptr(), payload_address);
    assert_eq!(packet.data.as_ref(), b"same");
    assert_eq!(packet.peer_identity.as_deref(), Some("actual-peer"));
    assert_eq!(
        packet.data.allocation_bytes(),
        64,
        "the complete original allocation remains responsible"
    );
    assert!(matches!(read.try_buffer(), Err(PacketMemoryError::Busy)));
    drop(packet);
    assert_eq!(root.driver_metadata().occupied, baseline);
    let actual_returned_workspace = read.try_buffer().unwrap();
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 64
        }
    );
    drop(actual_returned_workspace);
    read.revoke();
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 0,
            bytes: 0
        }
    );
}
