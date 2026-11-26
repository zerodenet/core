use super::*;
use std::sync::atomic::AtomicU64;

#[derive(Debug)]
struct NoopIoObserver;
impl zero_traits::IoObserver for NoopIoObserver {
    fn received(&self, _bytes: usize) {}
    fn sent(&self, _bytes: usize) {}
    fn error(&self) {}
    fn dropped(&self) {}
}

fn options(addresses: &[(&str, &str)]) -> TunIngressOptions {
    TunIngressOptions {
        addresses: addresses
            .iter()
            .map(|(addr, mask)| (addr.parse().unwrap(), mask.parse().unwrap()))
            .collect(),
        mtu: 1400,
        dns_hijack: true,
        dns_hijacked_queries: Arc::new(AtomicU64::new(0)),
        native_io: Arc::new(NoopIoObserver),
    }
}

#[test]
fn single_stack_ipv4_tun_rejects_ipv6_multicast_broadcast_and_link_local_noise() {
    let policy = options(&[("10.66.0.1", "255.255.255.0")]);

    assert!(policy.supports_family("1.1.1.1".parse().unwrap()));
    assert!(!policy.supports_family("ff02::1:3".parse().unwrap()));
    assert!(!policy.supports_family("2001:db8::1".parse().unwrap()));

    assert!(!policy.permits_routed_destination("ff02::1:3".parse().unwrap(), false));
    assert!(!policy.permits_routed_destination("224.0.0.252".parse().unwrap(), false));
    assert!(!policy.permits_routed_destination("255.255.255.255".parse().unwrap(), false));
    assert!(!policy.permits_routed_destination("10.66.0.255".parse().unwrap(), false));
    assert!(!policy.permits_routed_destination("169.254.1.10".parse().unwrap(), false));
    assert!(!policy.permits_routed_destination("0.0.0.0".parse().unwrap(), false));
    assert!(!policy.permits_routed_destination("10.66.0.1".parse().unwrap(), true));

    assert!(policy.permits_routed_destination("10.66.0.2".parse().unwrap(), false));
    assert!(policy.permits_routed_destination("198.18.0.1".parse().unwrap(), false));
    assert!(policy.permits_routed_destination("1.1.1.1".parse().unwrap(), false));
}

#[test]
fn dual_stack_tun_permits_unicast_ipv6_and_rejects_link_local_or_multicast_ipv6() {
    let policy = options(&[
        ("10.66.0.1", "255.255.255.0"),
        ("fd66::1", "ffff:ffff:ffff:ffff::"),
    ]);

    assert!(policy.permits_routed_destination("2001:db8::f".parse().unwrap(), false));
    assert!(!policy.permits_routed_destination("ff02::1:3".parse().unwrap(), false));
    assert!(!policy.permits_routed_destination("fe80::1".parse().unwrap(), false));
    assert!(!policy.permits_routed_destination("::".parse().unwrap(), false));
    assert!(!policy.permits_routed_destination("fd66::1".parse().unwrap(), true));
}
