use std::net::{IpAddr, Ipv4Addr};

use tokio::sync::mpsc;
use zero_stack::{packet, UserNetworkStack};
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
