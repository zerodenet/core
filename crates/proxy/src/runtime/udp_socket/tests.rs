use std::net::SocketAddr;

use zero_core::Address;

use super::{select_stable_udp_target, DirectUdpPolicy, DirectUdpSockets};
use zero_traits::{AddressFamily, DialPolicy};

mod policy;
mod ports;
mod replies;

#[test]
fn udp_target_selection_is_stable_across_same_family_answer_reordering() {
    let target = Address::Domain("Example.COM".to_owned());
    let first: Vec<SocketAddr> = [
        "192.0.2.10:443",
        "192.0.2.11:443",
        "192.0.2.12:443",
        "[2001:db8::10]:443",
    ]
    .into_iter()
    .map(|value| value.parse().unwrap())
    .collect();
    let reordered = vec![first[2], first[0], first[1], first[3]];

    let selected = select_stable_udp_target(&target, &first, true, true).unwrap();
    assert!(selected.is_ipv4());
    assert_eq!(
        select_stable_udp_target(&target, &reordered, true, true),
        Some(selected)
    );
}

#[test]
fn udp_target_selection_falls_back_to_an_available_family() {
    let target = Address::Domain("single-family.example".to_owned());
    let ipv4 = "192.0.2.10:443".parse().unwrap();
    let ipv6 = "[2001:db8::10]:443".parse().unwrap();
    let candidates = [ipv4, ipv6];

    assert_eq!(
        select_stable_udp_target(&target, &candidates, false, true),
        Some(ipv6)
    );
    assert_eq!(
        select_stable_udp_target(&target, &candidates, true, false),
        Some(ipv4)
    );
    assert_eq!(
        select_stable_udp_target(&target, &candidates, false, false),
        None
    );
}

fn proxy() -> crate::runtime::Proxy {
    crate::runtime::Proxy::new(config("auto", "auto")).unwrap()
}

fn config(first_family: &str, second_family: &str) -> zero_config::RuntimeConfig {
    zero_config::RuntimeConfig::parse(&format!(r#"{{
        "outbounds": [
            {{ "tag": "first", "protocol": {{ "type": "direct" }}, "dial": {{ "address_family": "{first_family}" }} }},
            {{ "tag": "second", "protocol": {{ "type": "direct" }}, "dial": {{ "address_family": "{second_family}" }} }}
        ],
        "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
    }}"#)).unwrap()
}

fn socket_set(proxy: &crate::runtime::Proxy, preferred_port: Option<u16>) -> DirectUdpSockets {
    DirectUdpSockets::new(
        crate::protocol_registry::UdpRuntimeServices::new(proxy.tcp_runtime_services()).network(),
        preferred_port,
    )
}

fn policy(proxy: &crate::runtime::Proxy, tag: Option<&str>) -> DirectUdpPolicy {
    let (dial_policy, generation) = proxy.engine().direct_dial_policy(tag).unwrap();
    DirectUdpPolicy {
        tag: tag.map(str::to_owned),
        dial_policy,
        generation,
    }
}

#[tokio::test]
async fn direct_udp_dispatcher_is_lazy_and_empty_receive_remains_pending() {
    let proxy = proxy();
    let sockets = socket_set(&proxy, None);
    assert!(sockets.sockets.is_empty());
    let mut buffer = [0; 16];
    assert!(tokio::time::timeout(
        std::time::Duration::from_millis(20),
        sockets.recv_from_addr(&mut buffer),
    )
    .await
    .is_err());
}

#[test]
fn direct_udp_target_selection_enforces_policy_and_canonicalizes_mapped_ipv4() {
    let target = Address::Domain("mixed.example".to_owned());
    let mapped = "[::ffff:192.0.2.10]:443".parse().unwrap();
    let ipv4 = "192.0.2.10:443".parse().unwrap();
    let ipv6 = "[2001:db8::10]:443".parse().unwrap();
    let v4_policy = DialPolicy {
        address_family: AddressFamily::OnlyIpv4,
        ..Default::default()
    };
    let v6_policy = DialPolicy {
        address_family: AddressFamily::OnlyIpv6,
        ..Default::default()
    };
    assert_eq!(
        DirectUdpSockets::select_target(&target, &[ipv6, mapped], &v4_policy).unwrap(),
        ipv4
    );
    assert_eq!(
        DirectUdpSockets::select_target(&target, &[mapped, ipv6], &v6_policy).unwrap(),
        ipv6
    );
    assert!(DirectUdpSockets::select_target(&target, &[mapped], &v6_policy).is_err());
    let source_policy = DialPolicy {
        source_ip: Some("192.0.2.1".parse().unwrap()),
        ..Default::default()
    };
    assert_eq!(
        DirectUdpSockets::select_target(&target, &[ipv6, ipv4], &source_policy).unwrap(),
        ipv4
    );
}

#[test]
fn direct_udp_binding_identity_includes_effective_source_and_egress() {
    let proxy = proxy();
    let base = super::DirectUdpSocketBinding {
        policy: policy(&proxy, Some("first")),
        ipv6: false,
        egress_generation: 7,
        source: Some("192.0.2.1:0".parse().unwrap()),
        egress: Some(zero_platform_tokio::EgressInterface::new("physical0", 7).unwrap()),
    };
    let mut changed = base.clone();
    changed.source = Some("192.0.2.2:0".parse().unwrap());
    assert_ne!(base, changed);
    changed = base.clone();
    changed.egress_generation = 8;
    assert_ne!(base, changed);
    changed = base.clone();
    changed.egress = None;
    assert_ne!(base, changed);
    changed = base.clone();
    changed.policy = policy(&proxy, Some("second"));
    assert_ne!(base, changed);
}
