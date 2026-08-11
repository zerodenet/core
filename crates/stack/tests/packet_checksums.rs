use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use zero_stack::packet::{self, checksum, tcp_flags};

#[test]
fn ipv4_tcp_and_udp_packets_have_wire_valid_checksums() {
    let source = Ipv4Addr::new(203, 0, 113, 7);
    let destination = Ipv4Addr::new(10, 66, 0, 1);

    let tcp = packet::build_tcp_with_mss(
        IpAddr::V4(source),
        IpAddr::V4(destination),
        443,
        50_000,
        123,
        456,
        tcp_flags::SYN | tcp_flags::ACK,
        1360,
    );
    assert_eq!(checksum(&tcp[..20]), 0);
    assert_eq!(transport_checksum_v4(source, destination, 6, &tcp[20..]), 0);

    let udp = packet::build_udp(
        IpAddr::V4(source),
        IpAddr::V4(destination),
        53,
        50_001,
        b"dns-response",
    );
    assert_eq!(checksum(&udp[..20]), 0);
    assert_eq!(
        transport_checksum_v4(source, destination, 17, &udp[20..]),
        0
    );
}

#[test]
fn ipv6_tcp_and_udp_packets_have_wire_valid_checksums() {
    let source = "2001:db8::53".parse::<Ipv6Addr>().unwrap();
    let destination = "fd66::1".parse::<Ipv6Addr>().unwrap();

    let tcp = packet::build_tcp_with_mss(
        IpAddr::V6(source),
        IpAddr::V6(destination),
        443,
        50_000,
        123,
        456,
        tcp_flags::SYN | tcp_flags::ACK,
        1340,
    );
    assert_eq!(transport_checksum_v6(source, destination, 6, &tcp[40..]), 0);

    let udp = packet::build_udp(
        IpAddr::V6(source),
        IpAddr::V6(destination),
        53,
        50_001,
        b"dns-response",
    );
    assert_eq!(
        transport_checksum_v6(source, destination, 17, &udp[40..]),
        0
    );
}

fn transport_checksum_v4(
    source: Ipv4Addr,
    destination: Ipv4Addr,
    protocol: u8,
    transport: &[u8],
) -> u16 {
    let mut bytes = Vec::with_capacity(12 + transport.len());
    bytes.extend_from_slice(&source.octets());
    bytes.extend_from_slice(&destination.octets());
    bytes.extend_from_slice(&[0, protocol]);
    bytes.extend_from_slice(&(transport.len() as u16).to_be_bytes());
    bytes.extend_from_slice(transport);
    checksum(&bytes)
}

fn transport_checksum_v6(
    source: Ipv6Addr,
    destination: Ipv6Addr,
    protocol: u8,
    transport: &[u8],
) -> u16 {
    let mut bytes = Vec::with_capacity(40 + transport.len());
    bytes.extend_from_slice(&source.octets());
    bytes.extend_from_slice(&destination.octets());
    bytes.extend_from_slice(&(transport.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&[0, 0, 0, protocol]);
    bytes.extend_from_slice(transport);
    checksum(&bytes)
}
