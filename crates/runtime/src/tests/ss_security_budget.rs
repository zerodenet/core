use super::*;
#[test]
fn candidate_projection_preserves_distinct_security_units_and_omitted_shape() {
    let omitted = RuntimeOptionsConfig::default();
    let before = serde_json::to_value(&omitted).unwrap();
    let first = SsSecurityBudgetLimits::from_runtime(&omitted).unwrap();
    assert_eq!(first.tcp_salts, 500_000);
    assert_eq!(first.udp_receiver_windows, 8192);
    assert_eq!(first.udp_senders, 8192);
    assert_eq!(first.udp_associations, 8192);
    assert_eq!(first.udp_return_bindings, 65536);
    assert_eq!(first.udp_authenticated_codecs, 4096);
    assert_eq!(serde_json::to_value(&omitted).unwrap(), before);
    let mut candidate = omitted;
    candidate.ss_udp_max_senders = Some(1);
    let next = SsSecurityBudgetLimits::from_runtime(&candidate).unwrap();
    assert_ne!(first, next);
    assert_eq!(next.udp_receiver_windows, first.udp_receiver_windows);
    assert_eq!(next.udp_senders, 1);
    candidate.ss_udp_max_return_bindings = Some(0);
    assert!(SsSecurityBudgetLimits::from_runtime(&candidate).is_err());
    candidate.ss_udp_max_return_bindings = Some(usize::MAX);
    assert!(SsSecurityBudgetLimits::from_runtime(&candidate).is_err());
}
