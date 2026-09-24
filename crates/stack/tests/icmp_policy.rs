use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use zero_stack::packet::{self, build_icmp_response};

#[test]
fn ipv4_echo_is_forwardable_and_can_be_rejected_by_policy() {
    let request = ipv4_echo_request();
    assert!(packet::build_icmp_mtu_response(&request, 1_500).is_none());
    let response = packet::build_icmp_echo_unreachable_response(&request, 1_500)
        .expect("ICMP policy response");
    assert_eq!(&response[12..16], &[1, 1, 1, 1]);
    assert_eq!(&response[16..20], &[10, 0, 0, 2]);
    assert_eq!(response[20], 3);
    assert_eq!(response[21], 13);
    assert_eq!(packet::checksum(&response[20..]), 0);
}

#[test]
fn ipv6_echo_is_forwardable_and_can_be_rejected_by_policy() {
    let request = ipv6_echo_request();
    assert!(packet::build_icmp_mtu_response(&request, 1_280).is_none());
    let response = packet::build_icmp_echo_unreachable_response(&request, 1_280)
        .expect("ICMPv6 policy response");
    assert_eq!(response[40], 1);
    assert_eq!(response[41], 1);
    assert_eq!(&response[8..24], &Ipv6Addr::LOCALHOST.octets());
}

#[test]
fn ipv4_echo_probe_and_reply_restore_tunnel_addresses_and_identifier() {
    let packet = ipv4_echo_request();
    let request = packet::parse_icmp_echo_request(&packet).unwrap();
    let probe =
        packet::build_icmp_echo_probe(&request, 0x4567, IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)))
            .unwrap();
    assert_eq!(&probe[4..6], &[0x45, 0x67]);
    assert_eq!(packet::checksum(&probe), 0);
    let mut received = probe;
    received[0] = 0;
    received[2..4].fill(0);
    let checksum = packet::checksum(&received);
    received[2..4].copy_from_slice(&checksum.to_be_bytes());
    let mut forged = received.clone();
    forged[4] ^= 1;
    forged[2..4].fill(0);
    let checksum = packet::checksum(&forged);
    forged[2..4].copy_from_slice(&checksum.to_be_bytes());
    assert!(packet::build_icmp_echo_reply(
        &request,
        0x4567,
        &forged,
        IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)),
        1_500,
    )
    .is_none());
    let reply = packet::build_icmp_echo_reply(
        &request,
        0x4567,
        &received,
        IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)),
        1_500,
    )
    .unwrap();
    assert_eq!(&reply[12..16], &[1, 1, 1, 1]);
    assert_eq!(&reply[16..20], &[10, 0, 0, 2]);
    assert_eq!(&reply[24..26], &[0, 0]);
    assert_eq!(packet::checksum(&reply[20..]), 0);
}

#[test]
fn ipv4_tunnel_probe_uses_outbound_address_and_parses_authenticated_reply() {
    let original = ipv4_echo_request();
    let request = packet::parse_icmp_echo_request(&original).unwrap();
    let local = Ipv4Addr::new(10, 9, 0, 2);
    let target = Ipv4Addr::new(1, 1, 1, 1);
    let probe =
        packet::build_icmp_echo_tunnel_probe(&request, 0x4567, IpAddr::V4(local), 1_420).unwrap();
    assert_eq!(&probe[12..16], &local.octets());
    assert_eq!(&probe[16..20], &target.octets());
    assert_eq!(&probe[24..26], &0x4567_u16.to_be_bytes());
    assert!(packet::build_icmp_echo_tunnel_probe(
        &request,
        0x4567,
        IpAddr::V4(local),
        probe.len() - 1,
    )
    .is_none());

    let mut reply = probe;
    reply[12..16].copy_from_slice(&target.octets());
    reply[16..20].copy_from_slice(&local.octets());
    reply[20] = 0;
    reply[22..24].fill(0);
    let checksum = packet::checksum(&reply[20..]);
    reply[22..24].copy_from_slice(&checksum.to_be_bytes());
    reply[10..12].fill(0);
    let checksum = packet::checksum(&reply[..20]);
    reply[10..12].copy_from_slice(&checksum.to_be_bytes());
    let parsed = packet::parse_icmp_echo_reply(&reply).unwrap();
    assert_eq!(parsed.identifier, 0x4567);
    assert_eq!(parsed.source, IpAddr::V4(target));
    assert_eq!(parsed.destination, IpAddr::V4(local));
    reply[27] ^= 1;
    assert!(packet::parse_icmp_echo_reply(&reply).is_none());
}

#[test]
fn corrupt_echo_request_is_not_forwarded() {
    let mut request = ipv4_echo_request();
    request[27] ^= 1;
    assert!(packet::parse_icmp_echo_request(&request).is_none());
}

#[test]
fn ipv6_echo_probe_and_reply_restore_tunnel_addresses_and_identifier() {
    let packet = ipv6_echo_request();
    let request = packet::parse_icmp_echo_request(&packet).unwrap();
    let local = "2001:db8::99".parse::<Ipv6Addr>().unwrap();
    let target = Ipv6Addr::LOCALHOST;
    let probe = packet::build_icmp_echo_probe(&request, 0x4567, IpAddr::V6(local)).unwrap();
    assert_eq!(icmpv6_checksum(local, target, &probe), 0);
    let mut received = probe;
    received[0] = 129;
    received[2..4].fill(0);
    let checksum = icmpv6_checksum(target, local, &received);
    received[2..4].copy_from_slice(&checksum.to_be_bytes());
    let reply =
        packet::build_icmp_echo_reply(&request, 0x4567, &received, IpAddr::V6(local), 1_280)
            .unwrap();
    assert_eq!(&reply[8..24], &target.octets());
    assert_eq!(
        &reply[24..40],
        &"2001:db8::2".parse::<Ipv6Addr>().unwrap().octets()
    );
    assert_eq!(reply[40], 129);
    assert_eq!(
        icmpv6_checksum(target, "2001:db8::2".parse().unwrap(), &reply[40..]),
        0
    );
}

#[test]
fn ipv6_tunnel_probe_uses_outbound_address_and_validates_reply_checksum() {
    let original = ipv6_echo_request();
    let request = packet::parse_icmp_echo_request(&original).unwrap();
    let local: Ipv6Addr = "fd00::9".parse().unwrap();
    let target = Ipv6Addr::LOCALHOST;
    let mut reply =
        packet::build_icmp_echo_tunnel_probe(&request, 0x4567, IpAddr::V6(local), 1_280).unwrap();
    assert_eq!(&reply[8..24], &local.octets());
    assert_eq!(&reply[24..40], &target.octets());
    reply[8..24].copy_from_slice(&target.octets());
    reply[24..40].copy_from_slice(&local.octets());
    reply[40] = 129;
    reply[42..44].fill(0);
    let checksum = icmpv6_checksum(target, local, &reply[40..]);
    reply[42..44].copy_from_slice(&checksum.to_be_bytes());
    let parsed = packet::parse_icmp_echo_reply(&reply).unwrap();
    assert_eq!(parsed.identifier, 0x4567);
    assert_eq!(parsed.source, IpAddr::V6(target));
    assert_eq!(parsed.destination, IpAddr::V6(local));
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

#[test]
fn oversized_unfragmented_ipv6_packet_receives_packet_too_big() {
    let request = packet::build_udp(
        IpAddr::V6("2001:db8::2".parse().unwrap()),
        IpAddr::V6("2001:db8::1".parse().unwrap()),
        50_000,
        443,
        &[7; 1_400],
    );
    let response = build_icmp_response(&request, 1_280).expect("packet-too-big response");
    assert!(response.len() <= 1_280);
    assert_eq!(response[6], packet::IPPROTO_ICMPV6);
    assert_eq!(response[40], 2);
    assert_eq!(response[41], 0);
    assert_eq!(
        u32::from_be_bytes([response[44], response[45], response[46], response[47]]),
        1_280
    );
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
    let checksum = icmpv6_checksum(
        "2001:db8::2".parse().unwrap(),
        Ipv6Addr::LOCALHOST,
        &packet[40..],
    );
    packet[42..44].copy_from_slice(&checksum.to_be_bytes());
    packet
}

fn icmpv6_checksum(source: Ipv6Addr, destination: Ipv6Addr, message: &[u8]) -> u16 {
    let mut pseudo = Vec::new();
    pseudo.extend_from_slice(&source.octets());
    pseudo.extend_from_slice(&destination.octets());
    pseudo.extend_from_slice(&(message.len() as u32).to_be_bytes());
    pseudo.extend_from_slice(&[0, 0, 0, packet::IPPROTO_ICMPV6]);
    pseudo.extend_from_slice(message);
    packet::checksum(&pseudo)
}
#[test]
fn forwarded_packet_advances_one_ip_hop_without_changing_addresses() {
    let mut ipv4 = vec![0_u8; 28];
    ipv4[0] = 0x45;
    ipv4[2..4].copy_from_slice(&28_u16.to_be_bytes());
    ipv4[8] = 2;
    ipv4[9] = zero_stack::packet::IPPROTO_ICMP;
    ipv4[12..16].copy_from_slice(&[10, 0, 0, 2]);
    ipv4[16..20].copy_from_slice(&[10, 0, 0, 3]);
    let checksum = zero_stack::packet::checksum(&ipv4[..20]);
    ipv4[10..12].copy_from_slice(&checksum.to_be_bytes());
    assert!(zero_stack::packet::advance_ip_hop(&mut ipv4));
    assert_eq!(ipv4[8], 1);
    assert_eq!(zero_stack::packet::checksum(&ipv4[..20]), 0);
    assert_eq!(
        zero_stack::packet::ip_source(&ipv4).unwrap().to_string(),
        "10.0.0.2"
    );
    assert!(!zero_stack::packet::advance_ip_hop(&mut ipv4));

    let mut ipv6 = vec![0_u8; 40];
    ipv6[0] = 0x60;
    ipv6[7] = 2;
    ipv6[8..24].copy_from_slice(&"fd00::2".parse::<std::net::Ipv6Addr>().unwrap().octets());
    ipv6[24..40].copy_from_slice(&"fd00::3".parse::<std::net::Ipv6Addr>().unwrap().octets());
    assert!(zero_stack::packet::advance_ip_hop(&mut ipv6));
    assert_eq!(ipv6[7], 1);
    assert!(!zero_stack::packet::advance_ip_hop(&mut ipv6));
}

#[test]
fn expired_forwarded_echo_gets_time_exceeded_with_original_quote() {
    let mut request = vec![0_u8; 28];
    request[0] = 0x45;
    request[2..4].copy_from_slice(&28_u16.to_be_bytes());
    request[8] = 1;
    request[9] = zero_stack::packet::IPPROTO_ICMP;
    request[12..16].copy_from_slice(&[10, 0, 0, 2]);
    request[16..20].copy_from_slice(&[127, 0, 0, 1]);
    request[20] = 8;
    let icmp_checksum = zero_stack::packet::checksum(&request[20..]);
    request[22..24].copy_from_slice(&icmp_checksum.to_be_bytes());
    let ip_checksum = zero_stack::packet::checksum(&request[..20]);
    request[10..12].copy_from_slice(&ip_checksum.to_be_bytes());
    let response = zero_stack::packet::build_icmp_time_exceeded_response(
        &request,
        "10.9.0.2".parse().unwrap(),
        1_420,
    )
    .unwrap();
    assert_eq!(response[20], 11);
    assert_eq!(&response[28..], request.as_slice());
    assert_eq!(zero_stack::packet::checksum(&response[20..]), 0);
}
