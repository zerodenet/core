use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use zero_stack::packet;

use super::{probe, IcmpEchoRelay};

#[test]
fn direct_echo_claim_rejects_non_echo_icmp_packets() {
    let ipv4 = echo_request_v4();
    assert!(IcmpEchoRelay::accepts_direct_echo(&ipv4));
    let mut timestamp_request = ipv4;
    timestamp_request[20] = 13;
    timestamp_request[22..24].fill(0);
    let checksum = packet::checksum(&timestamp_request[20..]);
    timestamp_request[22..24].copy_from_slice(&checksum.to_be_bytes());
    assert!(!IcmpEchoRelay::accepts_direct_echo(&timestamp_request));

    let ipv6 = echo_request_v6();
    assert!(IcmpEchoRelay::accepts_direct_echo(&ipv6));
    let mut echo_reply = ipv6;
    echo_reply[40] = 129;
    assert!(!IcmpEchoRelay::accepts_direct_echo(&echo_reply));
}

#[tokio::test]
#[ignore = "requires live ICMP sockets"]
async fn direct_icmp_echo_probe_returns_real_ipv4_and_ipv6_loopback_replies() {
    let ipv4 = echo_request_v4();
    let reply = probe(&ipv4, 1_420, None)
        .await
        .expect("IPv4 probe I/O")
        .expect("IPv4 echo reply");
    assert_eq!(&reply[12..16], &Ipv4Addr::LOCALHOST.octets());
    assert_eq!(&reply[16..20], &[10, 0, 0, 2]);
    assert_eq!(reply[20], 0);

    let ipv6 = echo_request_v6();
    let reply = probe(&ipv6, 1_420, None)
        .await
        .expect("IPv6 probe I/O")
        .expect("IPv6 echo reply");
    assert_eq!(&reply[8..24], &Ipv6Addr::LOCALHOST.octets());
    assert_eq!(
        &reply[24..40],
        &"fd00::2".parse::<Ipv6Addr>().unwrap().octets()
    );
    assert_eq!(reply[40], 129);
}

fn echo_request_v4() -> Vec<u8> {
    let mut packet = vec![0_u8; 28];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&28_u16.to_be_bytes());
    packet[8] = 64;
    packet[9] = packet::IPPROTO_ICMP;
    packet[12..16].copy_from_slice(&[10, 0, 0, 2]);
    packet[16..20].copy_from_slice(&Ipv4Addr::LOCALHOST.octets());
    packet[20] = 8;
    let icmp_checksum = packet::checksum(&packet[20..]);
    packet[22..24].copy_from_slice(&icmp_checksum.to_be_bytes());
    let ip_checksum = packet::checksum(&packet[..20]);
    packet[10..12].copy_from_slice(&ip_checksum.to_be_bytes());
    packet
}

fn echo_request_v6() -> Vec<u8> {
    let source = "fd00::2".parse::<Ipv6Addr>().unwrap();
    let destination = Ipv6Addr::LOCALHOST;
    let mut packet = vec![0_u8; 48];
    packet[0] = 0x60;
    packet[4..6].copy_from_slice(&8_u16.to_be_bytes());
    packet[6] = packet::IPPROTO_ICMPV6;
    packet[7] = 64;
    packet[8..24].copy_from_slice(&source.octets());
    packet[24..40].copy_from_slice(&destination.octets());
    packet[40] = 128;
    let mut pseudo = Vec::new();
    pseudo.extend_from_slice(&source.octets());
    pseudo.extend_from_slice(&destination.octets());
    pseudo.extend_from_slice(&8_u32.to_be_bytes());
    pseudo.extend_from_slice(&[0, 0, 0, packet::IPPROTO_ICMPV6]);
    pseudo.extend_from_slice(&packet[40..]);
    let checksum = packet::checksum(&pseudo);
    packet[42..44].copy_from_slice(&checksum.to_be_bytes());
    assert!(packet::parse_icmp_echo_request(&packet).is_some());
    assert_eq!(
        packet::ip_destination(&packet),
        Some(IpAddr::V6(destination))
    );
    packet
}
