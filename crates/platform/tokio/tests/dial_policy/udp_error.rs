use super::*;
use std::error::Error;

#[test]
fn actual_native_udp_address_collision_keeps_its_original_os_error_through_phase_context() {
    // Retain the actual socket throughout: this is an OS collision, not a free
    // port probe or a reconstructed io::Error from an expected platform code.
    let held = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let address = held.local_addr().unwrap();
    let original = bind_native(address, false).unwrap_err();
    assert_eq!(original.kind(), io::ErrorKind::AddrInUse);
    let code = original
        .raw_os_error()
        .expect("actual native bind error code");
    let display = format!("bind_source: {original}");
    let contextual = phase("bind_source", original);
    assert_eq!(contextual.kind(), io::ErrorKind::AddrInUse);
    assert_eq!(contextual.to_string(), display);
    // A contextual io::Error is not itself the original OS error. The original
    // object must be recoverable via the standard source chain, not just text.
    assert_eq!(contextual.raw_os_error(), None);
    let retained = contextual
        .source()
        .unwrap()
        .downcast_ref::<io::Error>()
        .unwrap();
    assert_eq!(retained.kind(), io::ErrorKind::AddrInUse);
    assert_eq!(retained.raw_os_error(), Some(code));
    assert!(
        retained.get_ref().is_none(),
        "native OS representation was retained"
    );
    assert_eq!(held.local_addr().unwrap(), address);
}

#[tokio::test]
async fn public_udp_validation_retains_the_original_typed_policy_error() {
    let peer = "127.0.0.1:53".parse().unwrap();
    let selection = crate::EgressInterfaceControl::default().select_for_peer(peer);
    let policy = DialPolicy {
        interface: Some("invalid/interface".into()),
        ..Default::default()
    };
    let contextual = TokioDatagramSocket::bind_for_peer_with_policy(peer, &policy, &selection)
        .await
        .unwrap_err();
    assert_eq!(contextual.kind(), io::ErrorKind::InvalidInput);
    let original = contextual
        .source()
        .unwrap()
        .downcast_ref::<io::Error>()
        .unwrap();
    assert_eq!(original.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        original
            .get_ref()
            .unwrap()
            .downcast_ref::<zero_traits::DialPolicyError>(),
        Some(&zero_traits::DialPolicyError::InvalidInterface)
    );
    assert_eq!(
        contextual.to_string(),
        format!("validate_dial_policy: {original}")
    );
}
