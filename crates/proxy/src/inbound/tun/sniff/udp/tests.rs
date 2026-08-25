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
            destination,
            payload: b"ordinary datagram".to_vec(),
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
    assert_eq!(datagram.payload, b"ordinary datagram");
}
