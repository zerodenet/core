use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use super::*;

#[test]
fn rejected_udp_builds_bounded_ipv4_and_ipv6_errors() {
    let ipv4 = crate::packet::build_udp(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)),
        50_000,
        443,
        b"rejected-v4",
    );
    let response = build_udp_unreachable_response(&ipv4, 1500).unwrap();
    assert_eq!(response[9], IPPROTO_ICMP);
    assert_eq!(&response[12..16], &[203, 0, 113, 7]);
    assert_eq!(&response[16..20], &[10, 0, 0, 2]);
    assert_eq!(&response[20..22], &[3, 13]);
    assert!(response.len() <= 1500);

    let ipv6 = crate::packet::build_udp(
        IpAddr::V6("fd00::2".parse::<Ipv6Addr>().unwrap()),
        IpAddr::V6("2001:db8::7".parse::<Ipv6Addr>().unwrap()),
        50_001,
        443,
        b"rejected-v6",
    );
    let response = build_udp_unreachable_response(&ipv6, 1500).unwrap();
    assert_eq!(response[6], IPPROTO_ICMPV6);
    assert_eq!(&response[40..42], &[1, 1]);
    assert!(response.len() <= 1500);
}
