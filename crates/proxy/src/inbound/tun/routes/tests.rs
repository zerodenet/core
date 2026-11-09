use super::{
    next_retry, ReconcileTrigger, RouteRuntimeSpec, ROUTE_RETRY_DELAYS, ROUTE_WATCHDOG_INTERVAL,
};

#[test]
fn route_retry_backoff_starts_small_and_is_bounded() {
    assert_eq!(next_retry(None), 0);
    assert_eq!(next_retry(Some(0)), 1);
    assert_eq!(
        next_retry(Some(ROUTE_RETRY_DELAYS.len() - 1)),
        ROUTE_RETRY_DELAYS.len() - 1
    );
}

#[test]
fn only_platform_events_are_debounced() {
    assert!(ReconcileTrigger::PlatformEvent.should_debounce());
    assert!(!ReconcileTrigger::Retry.should_debounce());
    assert!(!ReconcileTrigger::Watchdog.should_debounce());
    assert!(ROUTE_WATCHDOG_INTERVAL > ROUTE_RETRY_DELAYS[0]);
}

#[test]
fn network_notifications_invalidate_caches_even_after_a_coalesced_link_flap() {
    assert!(ReconcileTrigger::PlatformEvent.invalidates_network(false));
    assert!(ReconcileTrigger::Manual.invalidates_network(false));
    assert!(!ReconcileTrigger::Watchdog.invalidates_network(false));
    assert!(!ReconcileTrigger::Retry.invalidates_network(false));
    assert!(ReconcileTrigger::Watchdog.invalidates_network(true));
}

#[test]
fn runtime_spec_tracks_only_managed_address_families() {
    let ipv4 = RouteRuntimeSpec {
        id: 1,
        tun_name: "tun0".to_owned(),
        recovery_key: "tun-in".to_owned(),
        primary_ipv6: false,
        addresses: vec![(
            "10.66.0.1".parse().unwrap(),
            "255.255.255.0".parse().unwrap(),
        )],
        include_cidrs: Vec::new(),
        exclude_cidrs: Vec::new(),
        dns_hijack: true,
        strict_route: true,
    };
    assert_eq!(ipv4.managed_families(), (true, false));

    let dual_stack = RouteRuntimeSpec {
        addresses: vec![
            ipv4.addresses[0],
            (
                "fd66::1".parse().unwrap(),
                "ffff:ffff:ffff:ffff::".parse().unwrap(),
            ),
        ],
        ..ipv4
    };
    assert_eq!(dual_stack.managed_families(), (true, true));
}
