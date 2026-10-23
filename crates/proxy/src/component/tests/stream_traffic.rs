use super::*;

#[test]
fn explicit_protocol_counts_replace_carrier_establishment_bytes_without_double_charging() {
    let traffic = EstablishmentTraffic::new(None);
    traffic.received(900);
    traffic.sent(1200);
    traffic.protocol_traffic(None, 0, 79);
    traffic.protocol_traffic(None, 0, 79);
    let result = traffic.finish();
    assert_eq!(result.read_bytes, 0);
    assert_eq!(result.written_bytes, 79);
    assert!(
        traffic.finish().is_empty(),
        "a completed result is consumed once"
    );
}

#[test]
fn explicit_zero_is_not_a_missing_report_and_legacy_roles_keep_the_original_fallback() {
    let explicit = EstablishmentTraffic::new(None);
    explicit.sent(500);
    explicit.protocol_traffic(None, 0, 0);
    assert!(explicit.finish().is_empty());
    assert!(
        explicit.finish().is_empty(),
        "carrier bytes were consumed too"
    );
    let legacy = EstablishmentTraffic::new(None);
    legacy.received(7);
    legacy.sent(11);
    let result = legacy.finish();
    assert_eq!(result.read_bytes, 7);
    assert_eq!(result.written_bytes, 11);
}
