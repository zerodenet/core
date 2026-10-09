use super::*;
#[test]
fn absent_interfaces_and_nonlocal_sources_fail_before_dialing() {
    let missing_interface = DialPolicy {
        interface: Some("zero-no-such-interface".into()),
        ..Default::default()
    };
    assert_eq!(
        validate_dial_policy(&missing_interface).unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );
    let missing_source = source_policy("192.0.2.254");
    assert_eq!(
        validate_dial_policy(&missing_source).unwrap_err().kind(),
        std::io::ErrorKind::AddrNotAvailable
    );
    let control = EgressInterfaceControl::default();
    let peer = "127.0.0.1:80".parse().unwrap();
    assert!(control
        .select_for_peer_with_policy(peer, &missing_interface)
        .is_err());
    assert!(control
        .select_for_peer_with_policy(peer, &missing_source)
        .is_err());
}
#[test]
fn incompatible_literal_family_is_rejected_before_route_selection() {
    let control = EgressInterfaceControl::default();
    let v4 = DialPolicy {
        address_family: AddressFamily::OnlyIpv4,
        ..Default::default()
    };
    let v6 = DialPolicy {
        address_family: AddressFamily::OnlyIpv6,
        ..Default::default()
    };
    assert!(control
        .select_for_peer_with_policy("[::1]:80".parse().unwrap(), &v4)
        .is_err());
    assert!(control
        .select_for_peer_with_policy("127.0.0.1:80".parse().unwrap(), &v6)
        .is_err());
    assert!(control
        .select_for_peer_with_policy("[::ffff:127.0.0.1]:80".parse().unwrap(), &v6)
        .is_err());
}
#[test]
fn source_binding_cannot_reenter_published_tun_address() {
    let control = EgressInterfaceControl::default();
    control.replace_tunnel_addresses(["127.0.0.1".parse().unwrap()]);
    let error = control
        .select_for_peer_with_policy("127.0.0.1:80".parse().unwrap(), &source_policy("127.0.0.1"))
        .unwrap_err();
    assert!(error.to_string().contains("TUN"));
}
#[test]
fn default_policy_retains_existing_loopback_route_behavior() {
    let control = EgressInterfaceControl::default();
    control.replace(Some(
        zero_platform_tokio::EgressInterface::new("synthetic-physical", 7).unwrap(),
    ));
    let peer = "127.0.0.1:80".parse().unwrap();
    let selection = control
        .select_for_peer_with_policy(peer, &DialPolicy::default())
        .unwrap();
    assert!(selection.interface().is_none());
    assert_eq!(selection.binding_reason(), EgressBindingReason::Loopback);
    assert_eq!(selection.dial_source_address(), None);
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn explicit_interface_survives_loopback_and_preserves_strict_mark() {
    let control = EgressInterfaceControl::default();
    let policy = loopback_policy();
    let peer = "127.0.0.1:80".parse().unwrap();
    let selection = control.select_for_peer_with_policy(peer, &policy).unwrap();
    let interface = selection
        .interface()
        .expect("explicit loopback binding retained");
    assert_eq!(Some(interface.name()), policy.interface.as_deref());
    assert_eq!(
        selection.binding_reason(),
        EgressBindingReason::ExplicitDialPolicy
    );
    control.replace(Some(interface.clone().with_socket_mark(0x1122).unwrap()));
    let selection = control.select_for_peer_with_policy(peer, &policy).unwrap();
    assert_eq!(selection.interface().unwrap().socket_mark(), Some(0x1122));
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn explicit_interface_conflicting_with_strict_route_fails_closed() {
    let control = EgressInterfaceControl::default();
    control.replace(Some(
        zero_platform_tokio::EgressInterface::new("different-physical", u32::MAX)
            .unwrap()
            .with_socket_mark(0x1122)
            .unwrap(),
    ));
    let error = control
        .select_for_peer_with_policy("127.0.0.1:80".parse().unwrap(), &loopback_policy())
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert!(error
        .to_string()
        .contains("conflicts with strict-route egress"));
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn source_only_policy_keeps_strict_route_identity_or_rejects_conflict() {
    let control = EgressInterfaceControl::default();
    let peer = "127.0.0.1:80".parse().unwrap();
    let explicit = control
        .select_for_peer_with_policy(peer, &loopback_policy())
        .unwrap();
    let strict = explicit
        .interface()
        .unwrap()
        .clone()
        .with_socket_mark(0x1122)
        .unwrap();
    control.replace(Some(strict.clone()));
    let policy = source_policy("127.0.0.1");
    let selection = control.select_for_peer_with_policy(peer, &policy).unwrap();
    assert_eq!(selection.interface(), Some(&strict));
    control.replace(Some(
        zero_platform_tokio::EgressInterface::new("different-physical", u32::MAX)
            .unwrap()
            .with_socket_mark(0x1122)
            .unwrap(),
    ));
    assert_eq!(
        control
            .select_for_peer_with_policy(peer, &policy)
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::AddrNotAvailable
    );
}
#[cfg(target_os = "linux")]
#[test]
fn local_source_must_belong_to_requested_interface() {
    let Ok(entries) = std::fs::read_dir("/sys/class/net") else {
        return;
    };
    let Some(name) = entries
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .find(|name| name != "lo")
    else {
        return;
    };
    let policy = DialPolicy {
        interface: Some(name),
        ..source_policy("127.0.0.1")
    };
    assert_eq!(
        validate_dial_policy(&policy).unwrap_err().kind(),
        std::io::ErrorKind::AddrNotAvailable
    );
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn active_tun_pins_source_owner_instead_of_trusting_unbound_probe() {
    let control = EgressInterfaceControl::default();
    control.replace_tunnel_addresses(["10.66.0.1".parse().unwrap()]);
    let selection = control
        .select_for_peer_with_policy("127.0.0.1:80".parse().unwrap(), &source_policy("127.0.0.1"))
        .unwrap();
    assert_eq!(
        selection.interface().map(|interface| interface.name()),
        loopback_policy().interface.as_deref()
    );
}
