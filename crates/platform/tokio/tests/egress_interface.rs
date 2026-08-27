use tokio::net::TcpListener;
use zero_platform_tokio::{
    EgressBindingReason, EgressInterface, EgressInterfaceControl, EgressRouteLookupStatus,
    TokioSocket,
};

#[test]
fn controller_replaces_and_clears_interface_atomically() {
    let controller = EgressInterfaceControl::default();
    let interface = EgressInterface::new("physical0", 7).expect("valid interface");

    assert!(controller.current().is_none());
    assert!(controller.replace(Some(interface.clone())).is_none());
    assert_eq!(controller.current(), Some(interface.clone()));
    assert_eq!(controller.replace(None), Some(interface));
    assert!(controller.current().is_none());
}

#[test]
fn generation_changes_only_when_published_topology_changes() {
    let controller = EgressInterfaceControl::default();
    let ipv4 = EgressInterface::new("ethernet", 7).expect("valid IPv4 interface");
    let ipv6 = EgressInterface::new("teredo", 14).expect("valid IPv6 interface");

    assert_eq!(controller.generation(), 0);
    controller.replace_for(false, Some(ipv4.clone()));
    assert_eq!(controller.generation(), 1);
    controller.replace_for(false, Some(ipv4));
    assert_eq!(controller.generation(), 1);

    controller.replace_for(true, Some(ipv6));
    assert_eq!(controller.generation(), 2);
    controller.replace_tunnel_addresses([
        "fd66::1".parse().unwrap(),
        "10.66.0.1".parse().unwrap(),
        "fd66::1".parse().unwrap(),
    ]);
    assert_eq!(controller.generation(), 3);
    controller.replace_tunnel_addresses(["10.66.0.1".parse().unwrap(), "fd66::1".parse().unwrap()]);
    assert_eq!(controller.generation(), 3);

    controller.clear();
    assert_eq!(controller.generation(), 4);
    controller.clear();
    assert_eq!(controller.generation(), 4);
}

#[test]
fn controller_selects_independent_ipv4_and_ipv6_interfaces() {
    let controller = EgressInterfaceControl::default();
    let ipv4 = EgressInterface::new("ethernet", 7).expect("valid IPv4 interface");
    let ipv6 = EgressInterface::new("teredo", 14).expect("valid IPv6 interface");

    controller.replace_for(false, Some(ipv4.clone()));
    controller.replace_for(true, Some(ipv6.clone()));
    assert_eq!(controller.current_for(false), Some(ipv4));
    assert_eq!(controller.current_for(true), Some(ipv6));

    controller.clear();
    assert!(controller.current_for(false).is_none());
    assert!(controller.current_for(true).is_none());
}

#[test]
fn peer_selection_preserves_a_more_specific_non_tun_route() {
    let (peer, route_source) = [
        ("0.0.0.0:0", "192.0.2.1:8000"),
        ("[::]:0", "[2001:db8::1]:8000"),
    ]
    .into_iter()
    .find_map(|(wildcard, peer)| {
        let peer = peer.parse::<std::net::SocketAddr>().ok()?;
        let probe = std::net::UdpSocket::bind(wildcard).ok()?;
        probe.connect(peer).ok()?;
        let source = probe.local_addr().ok()?.ip();
        (!source.is_unspecified() && !source.is_loopback()).then_some((peer, source))
    })
    .expect("host must expose an IPv4 or IPv6 route for route-selection tests");

    let controller = EgressInterfaceControl::default();
    let physical = EgressInterface::new("physical0", 7).expect("valid physical interface");
    controller.replace_for(peer.is_ipv6(), Some(physical.clone()));
    let candidates = if route_source.is_ipv6() {
        ["fd66::1", "fd66::2"]
    } else {
        ["10.66.0.1", "10.66.0.2"]
    };
    let non_route_source = candidates
        .into_iter()
        .map(|address| address.parse::<std::net::IpAddr>().unwrap())
        .find(|address| *address != route_source)
        .expect("one synthetic TUN address must differ from the route source");
    controller.replace_tunnel_addresses([non_route_source]);

    let system_selection = controller.select_for_peer(peer);
    assert!(system_selection.interface().is_none());
    assert_eq!(system_selection.route_source(), Some(route_source));
    assert_eq!(
        system_selection.route_lookup_status(),
        EgressRouteLookupStatus::Resolved
    );
    assert_eq!(
        system_selection.binding_reason(),
        EgressBindingReason::SystemRoute
    );

    controller.replace_tunnel_addresses([route_source]);
    let tun_selection = controller.select_for_peer(peer);
    assert_eq!(tun_selection.interface(), Some(&physical));
    assert_eq!(tun_selection.route_source(), Some(route_source));
    assert_eq!(
        tun_selection.binding_reason(),
        EgressBindingReason::TunRoute
    );
}

#[test]
fn peer_selection_is_fail_safe_before_tun_addresses_are_published() {
    let controller = EgressInterfaceControl::default();
    let physical = EgressInterface::new("physical0", 7).expect("valid physical interface");
    controller.replace_for(false, Some(physical.clone()));

    assert_eq!(
        controller.current_for_peer("192.0.2.1:443".parse().unwrap()),
        Some(physical)
    );
}

#[test]
fn peer_selection_rejects_active_tun_without_a_physical_egress() {
    let controller = EgressInterfaceControl::default();
    let peer = "192.0.2.1:443".parse().unwrap();
    controller.replace_tunnel_addresses(["10.66.0.1".parse().unwrap()]);

    let selection = controller.select_for_peer(peer);
    assert!(selection.interface().is_none());
    assert_eq!(
        selection.binding_reason(),
        EgressBindingReason::TunEgressUnavailable
    );
    assert!(selection.ensure_connectable().is_err());
    assert!(controller.try_current_for_peer(peer).is_err());
}

#[test]
fn wildcard_peer_is_not_mistaken_for_a_tun_route_probe() {
    let controller = EgressInterfaceControl::default();
    let wildcard = "0.0.0.0:0".parse().unwrap();

    let idle = controller.select_for_peer(wildcard);
    assert!(idle.interface().is_none());
    assert_eq!(idle.route_source(), None);
    assert_eq!(idle.route_lookup_status(), EgressRouteLookupStatus::Skipped);
    assert_eq!(
        idle.binding_reason(),
        EgressBindingReason::NoConfiguredInterface
    );
    assert!(idle.ensure_connectable().is_ok());

    controller.replace_tunnel_addresses(["10.66.0.1".parse().unwrap()]);
    let unavailable = controller.select_for_peer(wildcard);
    assert_eq!(
        unavailable.binding_reason(),
        EgressBindingReason::TunEgressUnavailable
    );
    assert!(unavailable.ensure_connectable().is_err());

    let physical = EgressInterface::new("physical0", 7).unwrap();
    controller.replace_for(false, Some(physical.clone()));
    let active = controller.select_for_peer(wildcard);
    assert_eq!(active.interface(), Some(&physical));
    assert_eq!(active.binding_reason(), EgressBindingReason::TunRoute);
    assert!(active.ensure_connectable().is_ok());
}

#[test]
fn interface_identity_rejects_incomplete_values() {
    assert!(EgressInterface::new("", 1).is_err());
    assert!(EgressInterface::new("physical0", 0).is_err());
    assert!(EgressInterface::new("physical0", 1)
        .unwrap()
        .with_socket_mark(0)
        .is_err());

    let marked = EgressInterface::new("physical0", 1)
        .unwrap()
        .with_socket_mark(0x1234_abcd)
        .unwrap();
    assert_eq!(marked.socket_mark(), Some(0x1234_abcd));
}

#[tokio::test]
async fn loopback_connection_does_not_bind_to_physical_egress() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let invalid_physical = EgressInterface::new("not-a-real-interface", u32::MAX).unwrap();

    let (client, accepted) = tokio::join!(
        TokioSocket::connect_addr_on(address, Some(&invalid_physical)),
        listener.accept()
    );
    let client = client.expect("loopback connection must ignore physical egress binding");
    assert!(client.egress_interface().is_none());
    accepted.expect("accept loopback connection");
}

#[tokio::test]
async fn loopback_datagram_does_not_bind_to_physical_egress() {
    let peer = "127.0.0.1:53".parse().unwrap();
    let invalid_physical = EgressInterface::new("not-a-real-interface", u32::MAX).unwrap();

    let socket =
        zero_platform_tokio::TokioDatagramSocket::bind_for_peer_on(peer, Some(&invalid_physical))
            .await
            .expect("loopback datagram must ignore physical egress binding");

    assert!(socket.local_addr().unwrap().is_ipv4());
    assert!(socket.egress_interface().is_none());
}
