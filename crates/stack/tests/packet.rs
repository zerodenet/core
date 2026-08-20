//! Tests for raw IP/TCP/UDP packet parsing, building, and checksums.
//!
//! Migrated from the inline `#[cfg(test)] mod tests` in
//! `crates/stack/src/packet.rs`.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use zero_stack::packet::{
    build_tcp, build_tcp_with_mss, build_udp, checksum, ip_protocol, parse_tcp, parse_udp,
    tcp_flags, IPPROTO_TCP, IPPROTO_UDP,
};

#[test]
fn parse_tcp_roundtrip_v4() {
    let p = build_tcp(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
        IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)),
        12345,
        443,
        100,
        0,
        tcp_flags::SYN,
        b"",
    );
    let t = parse_tcp(&p).expect("parse tcp v4");
    assert!(t.syn);
    assert_eq!(t.seq, 100);
    assert_eq!(t.src.port, 12345);
    assert_eq!(t.dst.port, 443);
}

#[test]
fn parse_tcp_roundtrip_v6() {
    let s = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1);
    let d = Ipv6Addr::new(0x2606, 0x4700, 0, 0, 0, 0, 0x6810, 0x1);
    let p = build_tcp(
        IpAddr::V6(s),
        IpAddr::V6(d),
        54321,
        443,
        500,
        0,
        tcp_flags::SYN,
        b"",
    );
    let t = parse_tcp(&p).expect("parse tcp v6");
    assert!(t.syn);
    assert_eq!(t.seq, 500);
}

#[test]
fn parses_peer_mss_from_syn_options() {
    let packet = build_tcp_with_mss(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
        IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)),
        12_345,
        443,
        100,
        0,
        tcp_flags::SYN,
        1_280,
    );
    assert_eq!(zero_stack::packet::tcp_mss(&packet), Some(1_280));
}

#[test]
fn parse_udp_roundtrip_v4() {
    let p = build_udp(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
        IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
        12345,
        53,
        b"dns query",
    );
    let u = parse_udp(&p).expect("parse udp v4");
    assert_eq!(u.dst.port, 53);
    assert_eq!(u.payload, b"dns query");
}

#[test]
fn parse_udp_roundtrip_v6() {
    let s = Ipv6Addr::LOCALHOST;
    let d = Ipv6Addr::LOCALHOST;
    let p = build_udp(IpAddr::V6(s), IpAddr::V6(d), 1, 53, b"dns");
    let u = parse_udp(&p).expect("parse udp v6");
    assert_eq!(u.dst.port, 53);
    assert_eq!(u.payload, b"dns");
}

#[test]
fn ip_protocol_detect() {
    let tcp = build_tcp(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        1,
        80,
        0,
        0,
        tcp_flags::SYN,
        b"",
    );
    assert_eq!(ip_protocol(&tcp), Some(IPPROTO_TCP));

    let udp = build_udp(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        1,
        53,
        b"x",
    );
    assert_eq!(ip_protocol(&udp), Some(IPPROTO_UDP));
}

#[test]
fn checksum_ip_header_known() {
    // Build a minimal valid IPv4 header and verify checksum is non-zero.
    let mut hdr = [0u8; 20];
    hdr[0] = 0x45;
    hdr[2] = 0;
    hdr[3] = 20; // total length
    hdr[8] = 64;
    hdr[9] = 6; // TCP
    hdr[12..16].copy_from_slice(&[192, 168, 1, 1]);
    hdr[16..20].copy_from_slice(&[10, 0, 0, 1]);
    let c = checksum(&hdr);
    assert_ne!(c, 0);
}

#[test]
fn rejects_truncated_and_inconsistent_ip_lengths() {
    assert!(parse_tcp(&[0x45; 19]).is_none());

    let mut tcp = build_tcp(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        1,
        80,
        0,
        0,
        tcp_flags::SYN,
        b"",
    );
    tcp[2..4].copy_from_slice(&u16::MAX.to_be_bytes());
    assert!(parse_tcp(&tcp).is_none());
}

#[test]
fn rejects_invalid_transport_header_lengths() {
    let mut tcp = build_tcp(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        1,
        80,
        0,
        0,
        tcp_flags::SYN,
        b"",
    );
    tcp[32] = 0x40;
    assert!(parse_tcp(&tcp).is_none());

    let mut udp = build_udp(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        1,
        53,
        b"payload",
    );
    udp[24..26].copy_from_slice(&7u16.to_be_bytes());
    assert!(parse_udp(&udp).is_none());
}

#[test]
fn parses_ipv6_tcp_after_hop_by_hop_options() {
    let source = Ipv6Addr::LOCALHOST;
    let destination = Ipv6Addr::new(0x2606, 0x4700, 0, 0, 0, 0, 0x6810, 1);
    let tcp = build_tcp(
        IpAddr::V6(source),
        IpAddr::V6(destination),
        12_345,
        443,
        500,
        0,
        tcp_flags::ACK,
        b"hello",
    );
    let mut packet = Vec::with_capacity(tcp.len() + 8);
    packet.extend_from_slice(&tcp[..40]);
    packet.extend_from_slice(&[IPPROTO_TCP, 0, 0, 0, 0, 0, 0, 0]);
    packet.extend_from_slice(&tcp[40..]);
    packet[6] = 0;
    let payload_length = u16::from_be_bytes([packet[4], packet[5]]) + 8;
    packet[4..6].copy_from_slice(&payload_length.to_be_bytes());

    assert_eq!(ip_protocol(&packet), Some(IPPROTO_TCP));
    assert_eq!(
        parse_tcp(&packet).map(|tcp| tcp.payload),
        Some(&b"hello"[..])
    );
}

#[test]
fn rejects_ip_fragments_without_reassembly() {
    let mut ipv4 = build_tcp(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
        12_345,
        443,
        500,
        0,
        tcp_flags::ACK,
        b"fragment",
    );
    ipv4[6..8].copy_from_slice(&0x2000_u16.to_be_bytes());
    assert_eq!(ip_protocol(&ipv4), None);
    assert!(parse_tcp(&ipv4).is_none());

    let tcp = build_tcp(
        IpAddr::V6(Ipv6Addr::LOCALHOST),
        IpAddr::V6(Ipv6Addr::UNSPECIFIED),
        12_345,
        443,
        500,
        0,
        tcp_flags::ACK,
        b"fragment",
    );
    let mut ipv6 = Vec::with_capacity(tcp.len() + 8);
    ipv6.extend_from_slice(&tcp[..40]);
    ipv6.extend_from_slice(&[IPPROTO_TCP, 0, 0, 1, 0, 0, 0, 1]);
    ipv6.extend_from_slice(&tcp[40..]);
    ipv6[6] = 44;
    let payload_length = u16::from_be_bytes([ipv6[4], ipv6[5]]) + 8;
    ipv6[4..6].copy_from_slice(&payload_length.to_be_bytes());
    assert_eq!(ip_protocol(&ipv6), None);
    assert!(parse_tcp(&ipv6).is_none());
}
