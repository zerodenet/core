use std::net::IpAddr;

use zero_stack::packet::{
    self, build_icmp_response, build_udp_unreachable_response, Endpoint, IcmpErrorKind,
};

fn ip(value: &str) -> IpAddr {
    value.parse().unwrap()
}

#[test]
fn parses_ipv4_unreachable_and_correlates_quoted_udp_tuple() {
    let request = packet::build_udp(ip("10.0.0.2"), ip("203.0.113.7"), 50_000, 53, b"query");
    let response = build_udp_unreachable_response(&request, 1_420).unwrap();
    let error = packet::parse_icmp_error(&response).unwrap();
    assert_eq!(
        error.kind,
        IcmpErrorKind::DestinationUnreachable { code: 13 }
    );
    assert_eq!(error.quoted_protocol, packet::IPPROTO_UDP);
    assert_eq!(
        error.quoted_source,
        Endpoint {
            ip: ip("10.0.0.2"),
            port: 50_000
        }
    );
    assert_eq!(
        error.quoted_destination,
        Endpoint {
            ip: ip("203.0.113.7"),
            port: 53
        }
    );
}

#[test]
fn parses_ipv6_packet_too_big_without_requiring_full_quoted_packet() {
    let request = packet::build_udp(
        ip("fd00::2"),
        ip("2001:db8::7"),
        50_001,
        443,
        &vec![0x42; 1_500],
    );
    let mut response = build_icmp_response(&request, 1_280).unwrap();
    response.truncate(40 + 8 + 40 + 8);
    let payload_len = response.len() - 40;
    response[4..6].copy_from_slice(&(payload_len as u16).to_be_bytes());
    response[42..44].fill(0);
    // The ICMPv6 checksum must reflect the intentionally short quote.
    let mut pseudo = Vec::new();
    pseudo.extend_from_slice(&response[8..24]);
    pseudo.extend_from_slice(&response[24..40]);
    pseudo.extend_from_slice(&(payload_len as u32).to_be_bytes());
    pseudo.extend_from_slice(&[0, 0, 0, packet::IPPROTO_ICMPV6]);
    pseudo.extend_from_slice(&response[40..]);
    response[42..44].copy_from_slice(&packet::checksum(&pseudo).to_be_bytes());

    let error = packet::parse_icmp_error(&response).unwrap();
    assert_eq!(error.kind, IcmpErrorKind::PacketTooBig { mtu: Some(1_280) });
    assert_eq!(error.quoted_source.ip, ip("fd00::2"));
    assert_eq!(error.quoted_source.port, 50_001);
    assert_eq!(error.quoted_destination.port, 443);
}

#[test]
fn rejects_bad_checksum_and_non_error_icmp() {
    let request = packet::build_udp(ip("10.0.0.2"), ip("203.0.113.7"), 50_000, 53, b"query");
    let mut response = build_udp_unreachable_response(&request, 1_420).unwrap();
    response[30] ^= 1;
    assert!(packet::parse_icmp_error(&response).is_none());
    assert!(packet::parse_icmp_error(&request).is_none());
}
