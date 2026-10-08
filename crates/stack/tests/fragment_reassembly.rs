use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::time::{Duration, Instant};

use zero_stack::packet::{self, fragment_ip_packet};
use zero_stack::{FragmentOutcome, FragmentReassembler, FragmentRejectReason};

#[test]
#[ignore = "manual microbenchmark; not a throughput acceptance test"]
fn measure_owned_unfragmented_stack_boundary() {
    use std::hint::black_box;
    const COUNT: usize = 20_000;
    let sample = packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "10.0.0.1".parse().unwrap(),
        50000,
        53,
        &[7; 1400],
    );
    let mut borrowed_times = Vec::new();
    let mut owned_times = Vec::new();
    for _ in 0..5 {
        // Both variants start with identical owned device buffers. Preparation
        // is outside the measured interval, as is kernel/crypto I/O.
        let borrowed: Vec<_> = (0..COUNT).map(|_| sample.clone()).collect();
        let owned: Vec<_> = (0..COUNT).map(|_| sample.clone()).collect();
        let mut reassembler = FragmentReassembler::new();
        let start = Instant::now();
        for buffer in borrowed {
            let FragmentOutcome::NotFragmented(packet) =
                reassembler.process(black_box(&buffer), Instant::now())
            else {
                panic!("not fragmented")
            };
            let packet = packet.to_vec();
            black_box(fragment_ip_packet(&packet, 1500, 1));
        }
        borrowed_times.push(start.elapsed().as_nanos());
        let start = Instant::now();
        for buffer in owned {
            let zero_stack::OwnedFragmentOutcome::Packet {
                packet,
                reassembled: false,
            } = reassembler.process_owned(black_box(buffer), Instant::now())
            else {
                panic!("not fragmented")
            };
            black_box(packet::fragment_ip_packet_owned(packet, 1500, 1));
        }
        owned_times.push(start.elapsed().as_nanos());
    }
    borrowed_times.sort_unstable();
    owned_times.sort_unstable();
    println!("unfragmented stack microbenchmark: packets={COUNT}, bytes={}, rounds=5, borrowed_median_ns={}, owned_median_ns={}, removed_payload_copy_bytes_per_round={}", sample.len(), borrowed_times[2], owned_times[2], 2 * COUNT * sample.len());
}

#[test]
fn owned_unfragmented_pipeline_preserves_the_buffer() {
    for (source, destination) in [("10.0.0.2", "10.0.0.1"), ("fd00::2", "fd00::1")] {
        let original = packet::build_udp(
            source.parse().unwrap(),
            destination.parse().unwrap(),
            50000,
            53,
            &[7; 1400],
        );
        let expected = original.clone();
        let pointer = original.as_ptr();
        let capacity = original.capacity();
        let zero_stack::OwnedFragmentOutcome::Packet {
            packet: owned,
            reassembled,
        } = FragmentReassembler::new().process_owned(original, Instant::now())
        else {
            panic!("unfragmented packet rejected")
        };
        assert!(!reassembled);
        let mut outgoing = packet::fragment_ip_packet_owned(owned, 1500, 37);
        assert_eq!(outgoing.len(), 1);
        let outgoing = outgoing.pop().unwrap();
        assert_eq!(outgoing.as_ptr(), pointer);
        assert_eq!(outgoing.capacity(), capacity);
        assert_eq!(outgoing, expected);
    }
}

#[test]
fn owned_fragmentation_and_reassembly_preserve_transport_data() {
    for (source, destination, mtu) in [("10.0.0.2", "10.0.0.1", 576), ("fd00::2", "fd00::1", 1280)]
    {
        let original = packet::build_udp(
            source.parse().unwrap(),
            destination.parse().unwrap(),
            50000,
            53,
            &[7; 4096],
        );
        let expected = fragment_ip_packet(&original, mtu, 37);
        let fragments = packet::fragment_ip_packet_owned(original, mtu, 37);
        assert_eq!(fragments, expected);
        let mut reassembler = FragmentReassembler::new();
        let mut completed = None;
        for fragment in fragments.into_iter().rev() {
            match reassembler.process_owned(fragment, Instant::now()) {
                zero_stack::OwnedFragmentOutcome::Pending => {}
                zero_stack::OwnedFragmentOutcome::Packet {
                    packet,
                    reassembled,
                } => {
                    assert!(reassembled);
                    completed = Some(packet);
                }
                zero_stack::OwnedFragmentOutcome::Rejected(reason) => panic!("rejected {reason:?}"),
            }
        }
        assert_eq!(
            packet::parse_udp(&completed.unwrap()).unwrap().payload,
            &[7; 4096]
        );
        assert_eq!(reassembler.buffered_bytes(), 0);
    }
}

#[test]
fn reads_destination_from_every_fragment_without_parsing_transport_header() {
    for (source, destination, mtu) in [
        (
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            576,
        ),
        (
            IpAddr::V6("fd00::2".parse().unwrap()),
            IpAddr::V6("fd00::1".parse().unwrap()),
            1280,
        ),
    ] {
        let packet = packet::build_udp(source, destination, 50000, 53, &[7; 4096]);
        let fragments = fragment_ip_packet(&packet, mtu, 37);
        assert!(fragments.len() > 1);
        for fragment in fragments {
            assert_eq!(packet::ip_destination(&fragment), Some(destination));
        }
    }
}

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
