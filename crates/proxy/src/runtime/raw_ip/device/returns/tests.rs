use std::net::{IpAddr, Ipv4Addr};

use tokio::sync::mpsc;

use super::PacketReturns;

#[test]
fn packet_returns_reject_overlapping_ingress_sources() {
    let routes = PacketReturns::default();
    let source = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
    let (first, _first_rx) = mpsc::channel(1);
    let (second, _second_rx) = mpsc::channel(1);
    routes.register(source, 1, first).unwrap();
    let error = routes.register(source, 2, second).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
}

#[tokio::test]
async fn packet_returns_deliver_by_original_destination() {
    let routes = PacketReturns::default();
    let source = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
    let (sender, mut receiver) = mpsc::channel(1);
    routes.register(source, 1, sender).unwrap();
    let mut reply = vec![0_u8; 28];
    reply[0] = 0x45;
    reply[2..4].copy_from_slice(&28_u16.to_be_bytes());
    reply[8] = 64;
    reply[12..16].copy_from_slice(&[127, 0, 0, 1]);
    reply[16..20].copy_from_slice(&[10, 0, 0, 2]);
    let checksum = zero_stack::packet::checksum(&reply[..20]);
    reply[10..12].copy_from_slice(&checksum.to_be_bytes());
    assert!(routes.deliver(&reply));
    let routed = receiver.recv().await.unwrap();
    assert_eq!(routed[8], 63);
    assert_eq!(&routed[12..20], &reply[12..20]);
}
