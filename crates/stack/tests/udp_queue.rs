use std::net::{IpAddr, Ipv4Addr};
use std::time::Instant;

use tokio::sync::mpsc;
use zero_stack::{packet, UserNetworkStack};
use zero_stack::{FragmentOutcome, FragmentReassembler};
use zero_traits::{NetworkStack, UdpStack};

#[tokio::test]
async fn udp_receive_waits_for_the_next_fed_packet() {
    let (outbound, _packets) = mpsc::channel(4);
    let stack = UserNetworkStack::new(outbound, 1440);
    let udp = stack.udp();
    let mut payload = [0_u8; 16];

    let (size, source, destination) = {
        let receive = udp.recv_from(&mut payload);
        tokio::pin!(receive);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut receive)
                .await
                .is_err()
        );

        let packet = packet::build_udp(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
            IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
            53000,
            53,
            b"dns",
        );
        udp.feed(&packet).await;
        receive.await.expect("receive UDP packet")
    };
    assert_eq!(&payload[..size], b"dns");
    assert_eq!(source.port, 53000);
    assert_eq!(destination.port, 53);
}

#[tokio::test]
async fn oversized_udp_response_is_fragmented_to_the_stack_mtu() {
    let (outbound, mut packets) = mpsc::channel(16);
    let stack = UserNetworkStack::new(outbound, 516);
    let udp = stack.udp();
    let payload = vec![0x42; 2_048];
    let source = zero_traits::SocketAddress::new(zero_traits::IpAddress::V4([1, 1, 1, 1]), 443);
    let destination =
        zero_traits::SocketAddress::new(zero_traits::IpAddress::V4([10, 0, 0, 2]), 50_000);

    udp.send_to(&payload, source, destination).await;

    let mut reassembler = FragmentReassembler::new();
    let mut complete = None;
    while let Ok(fragment) = packets.try_recv() {
        assert!(fragment.len() <= 576);
        match reassembler.process(&fragment, Instant::now()) {
            FragmentOutcome::Pending => {}
            FragmentOutcome::Reassembled(packet) => complete = Some(packet),
            _ => panic!("unexpected fragmented UDP response"),
        }
    }
    let complete = complete.expect("reassembled UDP response");
    assert_eq!(
        packet::parse_udp(&complete).expect("UDP packet").payload,
        payload
    );
}
