#[path = "support/echo.rs"]
mod echo;

use std::{
    io,
    net::IpAddr,
    time::{Duration, Instant},
};
use zero_stack::{echo_translation::EchoTranslation, packet};

fn addresses(v6: bool) -> (IpAddr, IpAddr, IpAddr) {
    if v6 {
        (
            "fd00::1".parse().unwrap(),
            "fd10::11".parse().unwrap(),
            "fd20::235".parse().unwrap(),
        )
    } else {
        (
            "10.0.0.1".parse().unwrap(),
            "10.10.0.11".parse().unwrap(),
            "192.168.1.235".parse().unwrap(),
        )
    }
}

#[test]
fn translated_echo_preserves_payload_sequence_headers_and_restores_source() {
    for v6 in [false, true] {
        let (source, local, target) = addresses(v6);
        let original = echo::request(source, target, 123, 456, b"actual-ping-payload");
        let now = Instant::now();
        let mut adapter = EchoTranslation::default();
        let probe = adapter.request(&original, local, 42, now).unwrap();
        let request = packet::parse_icmp_echo_request(&probe).unwrap();
        assert_eq!(request.source, local);
        assert_eq!(
            &request.message[6..],
            &packet::parse_icmp_echo_request(&original).unwrap().message[6..]
        );
        assert_eq!(
            packet::ip_hop_limit(&probe),
            packet::ip_hop_limit(&original)
        );
        let response = echo::reply(&probe);
        let (context, restored) = adapter.response(&response, now).unwrap();
        assert_eq!(context, 42);
        assert_eq!(restored, echo::reply(&original));
        assert!(
            adapter.response(&response, now).is_none(),
            "duplicate must not escape"
        );
    }
}

#[test]
fn overlapping_ingresses_with_identical_ping_ids_return_to_the_correct_context() {
    let (source, local, target) = addresses(false);
    let original = echo::request(source, target, 7, 1, b"same");
    let now = Instant::now();
    let mut adapter = EchoTranslation::default();
    let first = adapter.request(&original, local, 11, now).unwrap();
    let second = adapter.request(&original, local, 22, now).unwrap();
    assert_ne!(
        packet::echo_response_key(&echo::reply(&first)),
        packet::echo_response_key(&echo::reply(&second))
    );
    assert_eq!(adapter.response(&echo::reply(&second), now).unwrap().0, 22);
    assert_eq!(adapter.response(&echo::reply(&first), now).unwrap().0, 11);
}

#[test]
fn invalid_payload_and_checksum_do_not_consume_a_pending_request() {
    let (source, local, target) = addresses(false);
    let now = Instant::now();
    let mut adapter = EchoTranslation::default();
    let probe = adapter
        .request(
            &echo::request(source, target, 7, 1, b"original"),
            local,
            1,
            now,
        )
        .unwrap();
    let mut corrupt = echo::reply(&probe);
    corrupt[28] ^= 1;
    assert!(adapter.response(&corrupt, now).is_none());
    corrupt[22..24].fill(0);
    let checksum = packet::checksum(&corrupt[20..]);
    corrupt[22..24].copy_from_slice(&checksum.to_be_bytes());
    assert!(
        adapter.response(&corrupt, now).is_none(),
        "checksum-valid wrong payload"
    );
    assert!(adapter.response(&echo::reply(&probe), now).is_some());
}

#[test]
fn time_exceeded_and_mtu_errors_restore_the_quoted_request_for_both_families() {
    for v6 in [false, true] {
        let (source, local, target) = addresses(v6);
        let router: IpAddr = if v6 { "fd20::1" } else { "192.168.1.1" }.parse().unwrap();
        for mtu_error in [false, true] {
            let original = echo::request(source, target, 123, 456, &[7; 800]);
            let now = Instant::now();
            let mut adapter = EchoTranslation::default();
            let probe = adapter.request(&original, local, 9, now).unwrap();
            let error = if mtu_error {
                packet::build_icmp_response(&probe, 576).unwrap()
            } else {
                packet::build_icmp_time_exceeded_response(&probe, router, 576).unwrap()
            };
            let (_, restored) = adapter.response(&error, now).unwrap();
            assert_eq!(packet::ip_destination(&restored), Some(source));
            assert_eq!(packet::ip_source(&restored), packet::ip_source(&error));
            let offset = if v6 { 40 } else { 20 };
            assert_eq!(&restored[offset..offset + 2], &error[offset..offset + 2]);
            assert_eq!(
                &restored[offset + 4..offset + 8],
                &error[offset + 4..offset + 8]
            );
            assert_eq!(
                &restored[offset + 8..],
                &original[..restored.len() - offset - 8]
            );
            if let (IpAddr::V6(from), IpAddr::V6(to)) =
                (packet::ip_source(&restored).unwrap(), source)
            {
                assert_eq!(echo::v6_checksum(from, to, &restored[offset..]), 0);
            } else {
                assert_eq!(packet::checksum(&restored[offset..]), 0);
            }
        }
    }
}

#[test]
fn translation_expires_limits_capacity_and_clears_retired_device_state() {
    let (source, local, target) = addresses(false);
    let original = echo::request(source, target, 7, 1, b"bounded");
    let now = Instant::now();
    let mut adapter = EchoTranslation::default();
    let probe = adapter.request(&original, local, 0, now).unwrap();
    assert!(adapter
        .response(&echo::reply(&probe), now + Duration::from_secs(31))
        .is_none());
    for id in 0..1024 {
        adapter.request(&original, local, id, now).unwrap();
    }
    assert_eq!(
        adapter
            .request(&original, local, 0, now)
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    adapter.expire(now, |_| true);
    let probe = adapter.request(&original, local, 1, now).unwrap();
    adapter.clear();
    assert!(adapter.response(&echo::reply(&probe), now).is_none());
}

#[test]
fn translation_rejects_non_echo_and_mismatched_address_families() {
    let (source, local, target) = addresses(false);
    let mut adapter = EchoTranslation::default();
    let udp = packet::build_udp(source, target, 50000, 53, b"dns");
    assert_eq!(
        adapter
            .request(&udp, local, (), Instant::now())
            .unwrap_err()
            .kind(),
        io::ErrorKind::Unsupported
    );
    assert!(adapter
        .request(
            &echo::request(source, target, 1, 1, b"v4"),
            "fd00::1".parse().unwrap(),
            (),
            Instant::now()
        )
        .is_err());
}

#[test]
fn new_ping_identifier_has_an_independent_packet_conversation() {
    let (source, _, target) = addresses(false);
    let first = echo::request(source, target, 1, 1, b"ping");
    let next_sequence = echo::request(source, target, 1, 2, b"ping");
    let second_process = echo::request(source, target, 2, 1, b"ping");
    assert_eq!(
        packet::packet_conversation_key(&first),
        packet::packet_conversation_key(&next_sequence)
    );
    assert_ne!(
        packet::packet_conversation_key(&first),
        packet::packet_conversation_key(&second_process)
    );
    let error = packet::build_icmp_echo_unreachable_response(&first, 1500).unwrap();
    assert!(
        packet::packet_conversation_key(&error).is_some(),
        "other ICMP remains routable on Packet"
    );
}

#[test]
fn forwarded_ipv4_fragmentation_preserves_identification_and_respects_df() {
    let (source, _, target) = addresses(false);
    let mut original = echo::request(source, target, 123, 1, &[7; 1600]);
    original[4..6].copy_from_slice(&12345_u16.to_be_bytes());
    original[10..12].fill(0);
    let checksum = packet::checksum(&original[..20]);
    original[10..12].copy_from_slice(&checksum.to_be_bytes());
    assert!(packet::ipv4_fragmentation_allowed(&original));
    let fragments = packet::fragment_forwarded_packet(&original, 576);
    assert!(fragments.len() > 1);
    for fragment in fragments {
        assert_eq!(
            packet::parse_ip_fragment(&fragment)
                .unwrap()
                .key
                .identification,
            12345
        );
        assert!(fragment.len() <= 576);
    }
    original[6] |= 0x40;
    assert!(!packet::ipv4_fragmentation_allowed(&original));
    assert!(packet::fragment_forwarded_packet(&original, 576).is_empty());
    assert!(!packet::ipv4_fragmentation_allowed(&[]));
}
