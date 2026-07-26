use zero_core::{InboundMuxUdpTermination, InboundMuxUdpTerminationProbe, UdpContinuityKey};

#[test]
fn opaque_udp_continuity_key_is_bounded_and_preserves_identity() {
    let bytes = [1_u8, 2, 3, 4, 5, 6, 7, 8];
    let key = UdpContinuityKey::from_bytes(&bytes).expect("valid continuity key");

    assert_eq!(key.as_bytes(), bytes);
    assert!(UdpContinuityKey::from_bytes(&[]).is_none());
    assert!(UdpContinuityKey::from_bytes(&[1; UdpContinuityKey::MAX_LEN + 1]).is_none());
}

#[test]
fn mux_udp_termination_probe_distinguishes_detach_from_explicit_end() {
    let probe = InboundMuxUdpTerminationProbe::transport_attached();
    let observer = probe.clone();

    assert_eq!(
        observer.reason(),
        InboundMuxUdpTermination::TransportDetached
    );
    probe.mark_explicit_end();
    assert_eq!(observer.reason(), InboundMuxUdpTermination::ExplicitEnd);
}
