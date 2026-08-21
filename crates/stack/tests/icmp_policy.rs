use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use zero_stack::packet::{self, build_icmp_response};

#[test]
fn ipv4_echo_is_rejected_explicitly_instead_of_timing_out() {
    let request = ipv4_echo_request();
    let response = build_icmp_response(&request, 1_500).expect("ICMP response");
    assert_eq!(&response[12..16], &[1, 1, 1, 1]);
    assert_eq!(&response[16..20], &[10, 0, 0, 2]);
    assert_eq!(response[20], 3);
    assert_eq!(response[21], 13);
    assert_eq!(packet::checksum(&response[20..]), 0);
}

#[test]
fn ipv6_echo_is_rejected_explicitly_instead_of_timing_out() {
    let request = ipv6_echo_request();
    let response = build_icmp_response(&request, 1_280).expect("ICMPv6 response");
    assert_eq!(response[40], 1);
    assert_eq!(response[41], 1);
    assert_eq!(&response[8..24], &Ipv6Addr::LOCALHOST.octets());
}

#[test]
fn oversized_unfragmented_ipv4_packet_receives_mtu_signal() {
    let request = packet::build_udp(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
        IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
        50_000,
        443,
        &[7; 1_400],
    );
    let response = build_icmp_response(&request, 576).expect("fragmentation-needed response");
    assert!(response.len() <= 576);
    assert_eq!(response[20], 3);
    assert_eq!(response[21], 4);
    assert_eq!(u16::from_be_bytes([response[26], response[27]]), 576);
}

fn ipv4_echo_request() -> Vec<u8> {
    let mut packet = vec![0_u8; 28];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&28_u16.to_be_bytes());
    packet[8] = 64;
    packet[9] = packet::IPPROTO_ICMP;
    packet[12..16].copy_from_slice(&[10, 0, 0, 2]);
    packet[16..20].copy_from_slice(&[1, 1, 1, 1]);
    packet[20] = 8;
    let icmp_checksum = packet::checksum(&packet[20..]);
    packet[22..24].copy_from_slice(&icmp_checksum.to_be_bytes());
    let ip_checksum = packet::checksum(&packet[..20]);
    packet[10..12].copy_from_slice(&ip_checksum.to_be_bytes());
    packet
}

fn ipv6_echo_request() -> Vec<u8> {
    let mut packet = vec![0_u8; 48];
    packet[0] = 0x60;
    packet[4..6].copy_from_slice(&8_u16.to_be_bytes());
    packet[6] = packet::IPPROTO_ICMPV6;
    packet[7] = 64;
    packet[8..24].copy_from_slice(&"2001:db8::2".parse::<Ipv6Addr>().unwrap().octets());
    packet[24..40].copy_from_slice(&Ipv6Addr::LOCALHOST.octets());
    packet[40] = 128;
    packet
}
