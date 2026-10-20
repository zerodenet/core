use super::*;

#[test]
fn controlled_selection_preserves_the_original_snapshot_no_probe_cases() {
    let control = EgressInterfaceControl::default();
    let loopback: SocketAddr = "127.0.0.1:9".parse().unwrap();
    assert_eq!(
        control
            .select_packet_without_native_probe(loopback)
            .unwrap()
            .binding_reason(),
        EgressBindingReason::Loopback
    );
    let wildcard: SocketAddr = "0.0.0.0:9".parse().unwrap();
    assert_eq!(
        control
            .select_packet_without_native_probe(wildcard)
            .unwrap()
            .binding_reason(),
        EgressBindingReason::NoConfiguredInterface
    );
    control.replace_tunnel_addresses(["10.0.0.1".parse().unwrap()]);
    let unavailable = control
        .select_packet_without_native_probe(wildcard)
        .unwrap();
    assert_eq!(
        unavailable.binding_reason(),
        EgressBindingReason::TunEgressUnavailable
    );
    assert_eq!(
        unavailable.ensure_connectable().unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    control.clear();
    let interface = EgressInterface::new("declared-physical", 1).unwrap();
    control.replace(Some(interface.clone()));
    let actual = control
        .select_packet_without_native_probe("192.0.2.1:9".parse().unwrap())
        .unwrap();
    assert_eq!(actual.interface(), Some(&interface));
    assert_eq!(actual.generation(), control.generation());
    assert_eq!(
        actual.route_lookup_status(),
        EgressRouteLookupStatus::Skipped
    );
    assert_eq!(
        actual.binding_reason(),
        EgressBindingReason::TunAddressesUnavailable
    );
}
