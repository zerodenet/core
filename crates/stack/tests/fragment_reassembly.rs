use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::time::{Duration, Instant};

use zero_stack::packet::{self, fragment_ip_packet};
use zero_stack::{FragmentOutcome, FragmentReassembler, FragmentRejectReason};

#[test]
fn reassembles_out_of_order_ipv4_udp_fragments() {
    let payload = vec![0x5a; 4_096];
    let packet = packet::build_udp(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
        IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
        50_000,
        443,
        &payload,
    );
    let mut fragments = fragment_ip_packet(&packet, 576, 7);
    assert!(fragments.len() > 1);
    fragments.reverse();

    let mut reassembler = FragmentReassembler::new();
    let now = Instant::now();
    let mut complete = None;
    for fragment in &fragments {
        match reassembler.process(fragment, now) {
            FragmentOutcome::Pending => {}
            FragmentOutcome::Reassembled(packet) => complete = Some(packet),
            _ => panic!("unexpected fragment outcome"),
        }
    }
    let complete = complete.expect("reassembled packet");
    assert_eq!(
        packet::parse_udp(&complete).expect("UDP packet").payload,
        payload
    );
    assert_eq!(reassembler.pending_datagrams(), 0);
    assert_eq!(reassembler.buffered_bytes(), 0);
}

#[test]
fn reassembles_ipv6_udp_fragments() {
    let payload = vec![0xa5; 4_096];
    let packet = packet::build_udp(
        IpAddr::V6(Ipv6Addr::LOCALHOST),
        IpAddr::V6("2001:4860:4860::8888".parse().expect("IPv6 address")),
        50_001,
        443,
        &payload,
    );
    let fragments = fragment_ip_packet(&packet, 1_280, 11);
    assert!(fragments.len() > 1);

    let mut reassembler = FragmentReassembler::new();
    let now = Instant::now();
    let mut complete = None;
    for fragment in &fragments {
        match reassembler.process(fragment, now) {
            FragmentOutcome::Pending => {}
            FragmentOutcome::Reassembled(packet) => complete = Some(packet),
            _ => panic!("unexpected fragment outcome"),
        }
    }
    let complete = complete.expect("reassembled packet");
    assert_eq!(
        packet::parse_udp(&complete).expect("UDP packet").payload,
        payload
    );
}

#[test]
fn accepts_identical_duplicate_and_rejects_ambiguous_overlap() {
    let packet = packet::build_udp(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
        IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
        50_002,
        443,
        &[9; 1_200],
    );
    let fragments = fragment_ip_packet(&packet, 576, 13);
    let mut reassembler = FragmentReassembler::new();
    let now = Instant::now();
    assert!(matches!(
        reassembler.process(&fragments[0], now),
        FragmentOutcome::Pending
    ));
    assert!(matches!(
        reassembler.process(&fragments[0], now),
        FragmentOutcome::Pending
    ));

    let mut conflicting = fragments[0].clone();
    *conflicting.last_mut().expect("fragment payload") ^= 1;
    assert!(matches!(
        reassembler.process(&conflicting, now),
        FragmentOutcome::Rejected(FragmentRejectReason::Overlap)
    ));
    assert_eq!(reassembler.pending_datagrams(), 0);
}

#[test]
fn expires_incomplete_fragment_state() {
    let packet = packet::build_udp(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
        IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
        50_003,
        443,
        &[3; 1_200],
    );
    let fragments = fragment_ip_packet(&packet, 576, 17);
    let mut reassembler = FragmentReassembler::new();
    let now = Instant::now();
    assert!(matches!(
        reassembler.process(&fragments[0], now),
        FragmentOutcome::Pending
    ));
    assert_eq!(
        reassembler.cleanup_expired(now + Duration::from_secs(31)),
        1
    );
    assert_eq!(reassembler.pending_datagrams(), 0);
    assert_eq!(reassembler.buffered_bytes(), 0);
}
