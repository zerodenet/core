use crate::echo;
use std::time::Instant;
use zero_stack::{echo_translation::EchoTranslation, packet};

#[test]
fn ipv4_translation_preserves_options_and_rejects_bad_ip_checksums() {
    let source = "10.0.0.1".parse().unwrap();
    let local = "10.10.0.11".parse().unwrap();
    let target = "192.168.1.235".parse().unwrap();
    let mut original = echo::request(source, target, 1, 2, b"ipv4-options");
    original.splice(20..20, [1, 1, 1, 0]);
    original[0] = 0x46;
    let size = original.len() as u16;
    original[2..4].copy_from_slice(&size.to_be_bytes());
    original[10..12].fill(0);
    let checksum = packet::checksum(&original[..24]);
    original[10..12].copy_from_slice(&checksum.to_be_bytes());
    let now = Instant::now();
    let mut adapter = EchoTranslation::default();
    let probe = adapter.request(&original, local, (), now).unwrap();
    assert_eq!(&probe[20..24], &original[20..24]);
    assert_eq!(probe[0], 0x46);
    let mut response = echo::reply(&probe);
    response[10] ^= 1;
    assert!(adapter.response(&response, now).is_none());
    assert!(adapter.response(&echo::reply(&probe), now).is_some());
    original[10] ^= 1;
    assert!(adapter.request(&original, local, (), now).is_err());
}

#[test]
fn ipv6_translation_preserves_hop_by_hop_header_and_restores_error_quotes() {
    let source = "fd00::1".parse().unwrap();
    let local = "fd10::11".parse().unwrap();
    let target = "fd20::235".parse().unwrap();
    let mut original = echo::request(source, target, 1, 2, b"ipv6-options");
    original.splice(40..40, [58, 0, 1, 4, 0, 0, 0, 0]);
    original[6] = 0;
    let size = (original.len() - 40) as u16;
    original[4..6].copy_from_slice(&size.to_be_bytes());
    let now = Instant::now();
    let mut adapter = EchoTranslation::default();
    let probe = adapter.request(&original, local, (), now).unwrap();
    assert_eq!(&probe[40..48], &original[40..48]);
    assert_eq!(probe[6], 0);
    let error = packet::build_icmp_time_exceeded_response(&probe, target, 1280).unwrap();
    let (_, restored) = adapter.response(&error, now).unwrap();
    assert_eq!(&restored[48..], &original);
    assert_eq!(packet::ip_destination(&restored), Some(source));
}

#[test]
fn error_quoting_the_first_ipv4_fragment_restores_the_original_ping_identity() {
    let source = "10.0.0.1".parse().unwrap();
    let local = "10.10.0.11".parse().unwrap();
    let target = "192.168.1.235".parse().unwrap();
    let original = echo::request(source, target, 123, 456, &[7; 1600]);
    let now = Instant::now();
    let mut adapter = EchoTranslation::default();
    let probe = adapter.request(&original, local, (), now).unwrap();
    let fragments = packet::fragment_forwarded_packet(&probe, 576);
    let error = packet::build_icmp_time_exceeded_response(&fragments[0], target, 576).unwrap();
    let (_, restored) = adapter.response(&error, now).unwrap();
    assert_eq!(packet::ip_destination(&restored), Some(source));
    assert_eq!(&restored[28..], &original[..restored.len() - 28]);
}
