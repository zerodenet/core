use crate::{Address, InboundUdpDispatch, ProtocolType, TargetHostSource};

#[test]
fn transparent_domain_preserves_original_target_metadata() {
    let original = Address::Ipv6([0x20, 1, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let dispatch = InboundUdpDispatch::new(
        ProtocolType::UNKNOWN,
        Address::Domain("mail.example".to_owned()),
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
