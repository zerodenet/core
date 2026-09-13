use alloc::{string::String, vec};

use crate::{Address, InboundUdpDispatch, ProtocolType, TargetHostSource};

#[test]
fn transparent_domain_preserves_original_target_metadata() {
    let original = Address::Ipv6([0x20, 1, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let dispatch = InboundUdpDispatch::new(
        ProtocolType::UNKNOWN,
        Address::Domain(String::from("mail.example")),
        443,
        vec![1, 2, 3],
        None,
    )
    .with_transparent_domain(original.clone(), TargetHostSource::QuicSni);

    assert!(dispatch.transparent_target());
    assert_eq!(dispatch.transparent_original_target(), Some(&original));
    assert_eq!(
        dispatch.transparent_host_source(),
        Some(TargetHostSource::QuicSni)
    );
}

#[test]
fn sniffed_udp_route_only_preserves_dial_target_and_payload() {
    let original = Address::Ipv4([203, 0, 113, 9]);
    let dispatch = InboundUdpDispatch::new(
        ProtocolType::UNKNOWN,
        original.clone(),
        443,
        vec![1, 2, 3],
        Some(7),
    )
    .with_sniffed_domain(
        String::from("quic.example"),
        TargetHostSource::QuicSni,
        true,
    );

    assert_eq!(dispatch.target(), &original);
    assert_eq!(
        dispatch.route_target(),
        Some(&Address::Domain(String::from("quic.example")))
    );
    assert_eq!(dispatch.payload(), &[1, 2, 3]);
    assert_eq!(dispatch.client_session_id(), Some(7));
}
