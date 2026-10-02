use super::*;
#[test]
fn macos_interface_statistics_are_real_64_bit_reads() {
    let result = read().unwrap();
    assert!(!result.is_empty());
    assert!(result.iter().any(|s| s.name == "lo0"));
    for sample in result {
        assert!(sample.index > 0);
        assert!(sample.rx_dropped_packets.is_some());
        assert!(sample.tx_dropped_packets.is_none());
    }
}
#[test]
fn malformed_route_records_are_rejected() {
    assert!(parse(&[0, 0, 0, 0]).is_err());
    assert!(parse(&[0]).is_err());
    assert!(parse(&[10, 0, libc::RTM_VERSION as u8, libc::RTM_IFINFO2 as u8]).is_err());
}
