use zero_core::{
    channel::{DeliveryState, RetryClass, SendReceipt},
    component::{ConfigRevision, EndpointId, NetworkGeneration, ResourceGeneration},
    packet::{InvalidIpPacket, IpFamily, IpPacket, ReturnAssociation},
};

fn association() -> ReturnAssociation {
    ReturnAssociation {
        endpoint: EndpointId(1),
        generation: ResourceGeneration(7),
        peer: None,
        correlation: 3,
    }
}

#[test]
fn generations_exhaust_instead_of_reusing_an_old_identity() {
    assert_eq!(ConfigRevision(0).checked_next(), Some(ConfigRevision(1)));
    assert_eq!(ConfigRevision(u64::MAX).checked_next(), None);
    assert_eq!(NetworkGeneration(u64::MAX).checked_next(), None);
    assert_eq!(ResourceGeneration(u64::MAX).checked_next(), None);
}

#[test]
fn authenticated_identity_debug_does_not_disclose_the_protocol_identifier() {
    let principal = zero_core::component::PrincipalId("opaque-private-identifier".into());
    assert_eq!(&*principal.0, "opaque-private-identifier");
    assert!(!format!("{principal:?}").contains("opaque-private"));
}

#[test]
fn retry_permission_requires_proof_of_no_submission() {
    for retry in [
        RetryClass::Never,
        RetryClass::BeforeSubmission,
        RetryClass::Revalidate,
    ] {
        assert!(!retry.permits(DeliveryState::Unknown));
        assert!(!retry.permits(DeliveryState::LocallyAccepted));
    }
    assert!(RetryClass::BeforeSubmission.permits(DeliveryState::NotSubmitted));
    assert!(!RetryClass::Never.permits(DeliveryState::NotSubmitted));
    assert_eq!(
        SendReceipt::accepted(0).delivery,
        DeliveryState::LocallyAccepted
    );
}

#[test]
fn packet_rejects_empty_truncated_and_concatenated_ip_envelopes() {
    assert_eq!(
        IpPacket::new(Vec::new().into(), association()).unwrap_err(),
        InvalidIpPacket::Header
    );
    let mut v4 = vec![0; 20];
    v4[0] = 0x45;
    v4[3] = 20;
    let packet = IpPacket::new(v4.clone().into(), association()).unwrap();
    assert_eq!(packet.family(), IpFamily::Ipv4);
    assert_eq!(packet.payload(), v4);
    v4.push(0);
    assert_eq!(
        IpPacket::new(v4.into(), association()).unwrap_err(),
        InvalidIpPacket::Length
    );
    let mut v6 = vec![0; 41];
    v6[0] = 0x60;
    v6[5] = 1;
    assert_eq!(
        IpPacket::new(v6.clone().into(), association())
            .unwrap()
            .family(),
        IpFamily::Ipv6
    );
    v6.pop();
    assert_eq!(
        IpPacket::new(v6.into(), association()).unwrap_err(),
        InvalidIpPacket::Length
    );
}
