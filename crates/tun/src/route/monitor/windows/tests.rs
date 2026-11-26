use super::*;
use windows_sys::Win32::NetworkManagement::IpHelper::MibAddInstance;
use windows_sys::Win32::Networking::WinSock::MIB_IPPROTO_NETMGMT;

#[test]
fn relevant_route_filter_skips_loopback_link_local_and_multicast_noise() {
    assert!(is_relevant_route_prefix(
        "0.0.0.0".parse().unwrap(),
        0,
        false,
        MIB_IPPROTO_NETMGMT,
    ));
    assert!(is_relevant_route_prefix(
        "0.0.0.0".parse().unwrap(),
        1,
        false,
        MIB_IPPROTO_NETMGMT,
    ));
    assert!(is_relevant_route_prefix(
        "128.0.0.0".parse().unwrap(),
        1,
        false,
        MIB_IPPROTO_NETMGMT,
    ));
    assert!(is_relevant_route_prefix(
        "::".parse().unwrap(),
        0,
        false,
        MIB_IPPROTO_NETMGMT,
    ));
    assert!(is_relevant_route_prefix(
        "1.1.1.1".parse().unwrap(),
        32,
        false,
        MIB_IPPROTO_NETMGMT,
    ));
    assert!(is_relevant_route_prefix(
        "2606:4700:4700::1111".parse().unwrap(),
        128,
        false,
        MIB_IPPROTO_NETMGMT,
    ));

    assert!(!is_relevant_route_prefix(
        "10.66.0.1".parse().unwrap(),
        32,
        false,
        MIB_IPPROTO_LOCAL,
    ));
    assert!(!is_relevant_route_prefix(
        "10.66.0.255".parse().unwrap(),
        32,
        false,
        MIB_IPPROTO_LOCAL,
    ));
    assert!(!is_relevant_route_prefix(
        "1.1.1.1".parse().unwrap(),
        32,
        false,
        MIB_IPPROTO_ICMP,
    ));
    assert!(!is_relevant_route_prefix(
        "1.1.1.1".parse().unwrap(),
        32,
        true,
        MIB_IPPROTO_NETMGMT,
    ));
    assert!(!is_relevant_route_prefix(
        "127.0.0.1".parse().unwrap(),
        32,
        false,
        MIB_IPPROTO_NETMGMT,
    ));
    assert!(!is_relevant_route_prefix(
        "::1".parse().unwrap(),
        128,
        false,
        MIB_IPPROTO_NETMGMT,
    ));
    assert!(!is_relevant_route_prefix(
        "169.254.10.1".parse().unwrap(),
        32,
        false,
        MIB_IPPROTO_NETMGMT,
    ));
    assert!(!is_relevant_route_prefix(
        "fe80::1".parse().unwrap(),
        64,
        false,
        MIB_IPPROTO_NETMGMT,
    ));
    assert!(!is_relevant_route_prefix(
        "224.0.0.0".parse().unwrap(),
        4,
        false,
        MIB_IPPROTO_NETMGMT,
    ));
    assert!(!is_relevant_route_prefix(
        "255.255.255.255".parse().unwrap(),
        32,
        false,
        MIB_IPPROTO_NETMGMT,
    ));
    assert!(!is_relevant_route_prefix(
        "ff02::1:3".parse().unwrap(),
        128,
        false,
        MIB_IPPROTO_NETMGMT,
    ));
}

#[test]
fn topology_cache_ignores_noop_parameter_refreshes_and_signals_real_link_flaps() {
    let mut cache = TopologyCache::default();
    let key = (42_u64, AF_INET);
    let up = InterfaceState {
        connected: true,
        metric: 25,
        nl_mtu: 1500,
    };
    assert!(cache.observe_interface(key, MibAddInstance, Some(up)));
    assert!(!cache.observe_interface(key, MibParameterNotification, Some(up)));

    // Sub-millisecond link flap (true -> false -> true) signals both transitions.
    let down = InterfaceState {
        connected: false,
        ..up
    };
    assert!(cache.observe_interface(key, MibParameterNotification, Some(down)));
    assert!(cache.observe_interface(key, MibParameterNotification, Some(up)));
    assert!(!cache.observe_interface(key, MibParameterNotification, Some(up)));

    // Metric or MTU updates signal once and then stabilize.
    let remetric = InterfaceState { metric: 5, ..up };
    assert!(cache.observe_interface(key, MibParameterNotification, Some(remetric)));
    assert!(!cache.observe_interface(key, MibParameterNotification, Some(remetric)));
    assert!(cache.observe_interface(key, MibDeleteInstance, None));

    let route = RouteKey {
        interface_index: 11,
        prefix: "0.0.0.0".parse().unwrap(),
        prefix_length: 0,
        next_hop: "192.168.1.1".parse().unwrap(),
    };
    assert!(cache.observe_route(route, 25, MibAddInstance));
    assert!(!cache.observe_route(route, 25, MibParameterNotification));
    assert!(cache.observe_route(route, 10, MibParameterNotification));
    assert!(cache.observe_route(route, 10, MibDeleteInstance));
}
