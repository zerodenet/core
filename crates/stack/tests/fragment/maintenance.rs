use super::*;
use std::net::{IpAddr, Ipv4Addr};

fn fragments(index: u16) -> Vec<Vec<u8>> {
    let packet = crate::packet::build_udp(
        IpAddr::V4(Ipv4Addr::new(10, 0, (index >> 8) as u8, index as u8)),
        "10.1.0.1".parse().unwrap(),
        40000,
        53,
        &[0x42; 192],
    );
    crate::packet::fragment_ip_packet(&packet, 100, u32::from(index))
}

#[test]
fn expiry_reclaims_burst_capacity_and_bytes_while_preserving_a_refreshed_assembly() {
    let now = Instant::now();
    let mut table = FragmentReassembler::new();
    for index in 0..MAX_FRAGMENT_ASSEMBLIES as u16 {
        assert!(matches!(
            table.process(&fragments(index)[0], now),
            FragmentOutcome::Pending
        ));
    }
    let peak = table.assemblies.capacity();
    let live = fragments(0);
    assert!(matches!(
        table.process(&live[0], now + Duration::from_secs(1)),
        FragmentOutcome::Pending
    ));
    let live_bytes = parse_ip_fragment(&live[0]).unwrap().payload.len();
    assert_eq!(
        table.cleanup_expired(now + FRAGMENT_EXPIRY),
        MAX_FRAGMENT_ASSEMBLIES - 1
    );
    assert_eq!(table.pending_datagrams(), 1);
    assert_eq!(table.buffered_bytes(), live_bytes);
    assert!(table.assemblies.capacity() < peak / 4);
    let mut complete = None;
    for packet in &live[1..] {
        match table.process(packet, now + FRAGMENT_EXPIRY) {
            FragmentOutcome::Pending => {}
            FragmentOutcome::Reassembled(packet) => complete = Some(packet),
            _ => panic!("live assembly must still complete"),
        }
    }
    assert_eq!(
        crate::packet::parse_udp(&complete.unwrap())
            .unwrap()
            .payload,
        &[0x42; 192]
    );
    assert_eq!(table.buffered_bytes(), 0);
    assert_eq!(table.pending_datagrams(), 0);
}

#[test]
fn expired_fragment_storage_releases_all_buckets_without_requiring_new_ingress() {
    let now = Instant::now();
    let mut table = FragmentReassembler::new();
    for index in 0..MAX_FRAGMENT_ASSEMBLIES as u16 {
        table.process(&fragments(index)[0], now);
    }
    assert_eq!(
        table.cleanup_expired(now + FRAGMENT_EXPIRY),
        MAX_FRAGMENT_ASSEMBLIES
    );
    assert_eq!(table.buffered_bytes(), 0);
    assert_eq!(table.assemblies.capacity(), 0);
    assert_eq!(table.cleanup_expired(now + FRAGMENT_EXPIRY), 0);
}
