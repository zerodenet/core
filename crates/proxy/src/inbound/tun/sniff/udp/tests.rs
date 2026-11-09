use tokio::sync::mpsc;
use zero_core::Address;
use zero_traits::{IpAddress, SocketAddress};

use super::TunQuicSniffer;
use crate::inbound::tun::udp::TunDatagram;

#[tokio::test]
async fn non_quic_udp_is_forwarded_without_sniff_delay() {
    let destination = SocketAddress::new(IpAddress::V4([203, 0, 113, 9]), 443);
    let (sender, mut receiver) = mpsc::channel(1);
    sender
        .send(TunDatagram {
            peer_identity: None,
            destination,
            payload: b"ordinary datagram".to_vec().into(),
        })
        .await
        .unwrap();

    let datagram = TunQuicSniffer::default()
        .next(&mut receiver)
        .await
        .expect("forward datagram");
    assert_eq!(datagram.original_destination, destination);
    assert_eq!(datagram.target, Address::Ipv4([203, 0, 113, 9]));
    assert!(datagram.host_source.is_none());
    assert_eq!(datagram.payload.as_ref(), b"ordinary datagram");
}

#[cfg(feature = "raw-ip-runtime")]
#[tokio::test]
async fn source_queue_association_sniffer_and_core_pipe_move_one_actual_paid_payload() {
    use std::sync::Arc;
    use zero_stack::{
        packet_output::{memory::PacketWireMemory, PacketSender},
        UserNetworkStack,
    };
    use zero_tun::packet_memory::{PacketMemoryLimit, PacketReturnTotals};
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
    let (output, _receive) = mpsc::channel::<zero_traits::PacketBuffer>(1);
    let sender = PacketSender::from(output)
        .with_packet_memory_view(view, PacketWireMemory::Native)
        .unwrap();
    let (_, stack) = UserNetworkStack::new_with_packet_output(sender, 1440).into_parts();
    let input = zero_stack::packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "203.0.113.9".parse().unwrap(),
        12000,
        443,
        b"ordinary datagram",
    );
    let mut incoming = read.try_buffer().unwrap();
    incoming.as_mut_bytes()[..input.len()].copy_from_slice(&input);
    let original = incoming
        .into_packet(input.len())
        .unwrap_or_else(|_| panic!("initialized packet length fits the original read workspace"));
    let input_payload_address = original[28..].as_ptr();
    stack.feed_owned_with_peer(original, None).await;
    let dequeued = stack.recv_datagram_with_peer().await.unwrap();
    let original_address = dequeued.data.as_ptr();
    assert_eq!(original_address, input_payload_address);
    assert!(read.try_buffer().is_err());
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 64
        }
    );
    let (sender, mut receiver) = mpsc::channel(1);
    sender
        .send(TunDatagram {
            peer_identity: dequeued.peer_identity,
            destination: dequeued.destination,
            payload: dequeued.data,
        })
        .await
        .unwrap();
    let datagram = TunQuicSniffer::default().next(&mut receiver).await.unwrap();
    assert_eq!(datagram.payload.as_ptr(), original_address);
    let dispatch = zero_core::InboundUdpDispatch::new_with_packet(
        zero_core::ProtocolType::UNKNOWN,
        datagram.target,
        datagram.original_destination.port,
        datagram.payload,
        None,
    )
    .with_transparent_target()
    .with_peer_identity(datagram.peer_identity);
    let pipe = crate::runtime::pipe::UdpPipeInput::from_inbound_dispatch(&dispatch, None, None);
    assert_eq!(pipe.payload.as_ptr(), original_address);
    assert_eq!(pipe.payload, b"ordinary datagram");
    assert!(pipe.transparent_target);
    assert_eq!(pipe.port, 443);
    assert!(root.driver_metadata().occupied.bytes() > baseline.bytes());
    drop(dispatch);
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 64
        }
    );
    assert_eq!(root.driver_metadata().occupied, baseline);
    assert!(
        read.try_buffer().is_ok(),
        "the same actual Native1 workspace was returned"
    );
    // This is allocation/custody evidence. It does not mint a Native/Scope
    // close receipt for the separate Source or its original driver.
}
