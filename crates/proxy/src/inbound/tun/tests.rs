use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use super::config::{
    configured_dns_endpoint_addresses, configured_dns_endpoint_addresses_with,
    parse_address_and_mask, parse_interface_addresses, DEFAULT_TUN_IPV4_ADDR,
    DEFAULT_TUN_IPV6_ADDR,
};
use super::{configured_tun_is_current, tun_route_exclusion_required, PreparedTunNetwork, TunInfo};

#[test]
fn route_cleanup_failure_retains_the_last_physical_egress() {
    let control = zero_platform_tokio::EgressInterfaceControl::default();
    let physical = zero_platform_tokio::EgressInterface::new("ethernet", 9).unwrap();
    control.replace_for(false, Some(physical.clone()));
    control.replace_tunnel_addresses(["10.66.0.1".parse().unwrap()]);

    super::clear_egress_after_route_cleanup(&control, false);

    assert_eq!(control.current_for(false), Some(physical));
    assert!(control
        .select_for_peer("192.0.2.1:443".parse().unwrap())
        .tun_active());
}

#[test]
fn completed_route_cleanup_clears_tun_egress_state() {
    let control = zero_platform_tokio::EgressInterfaceControl::default();
    control.replace_for(
        false,
        Some(zero_platform_tokio::EgressInterface::new("ethernet", 9).unwrap()),
    );
    control.replace_tunnel_addresses(["10.66.0.1".parse().unwrap()]);

    super::clear_egress_after_route_cleanup(&control, true);

    assert!(control.current().is_none());
    assert!(!control
        .select_for_peer("192.0.2.1:443".parse().unwrap())
        .tun_active());
}

#[tokio::test]
async fn unexpected_runtime_exit_waits_for_route_cleanup_before_clearing_egress() {
    let config =
        zero_config::RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#)
            .unwrap();
    let proxy = crate::Proxy::new(config).unwrap();
    proxy.egress_interface.replace_for(
        false,
        Some(zero_platform_tokio::EgressInterface::new("ethernet", 9).unwrap()),
    );
    proxy
        .egress_interface
        .replace_tunnel_addresses(["10.66.0.1".parse().unwrap()]);
    let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let (_done_tx, done) = tokio::sync::oneshot::channel();
    let (route_done_tx, route_done) = tokio::sync::oneshot::channel();
    *proxy.tun_control.lock().unwrap() = Some(crate::runtime::TunControl {
        id: 41,
        shutdown,
        done,
        route_done: Some(route_done),
    });

    let finalize = super::finalize_tun_runtime_exit(&proxy, 41);
    let acknowledge = async move {
        shutdown_rx.changed().await.unwrap();
        assert!(*shutdown_rx.borrow());
        route_done_tx.send(Ok(())).unwrap();
    };
    tokio::join!(finalize, acknowledge);

    assert!(proxy.egress_interface.current().is_none());
    assert!(!proxy
        .egress_interface
        .select_for_peer("192.0.2.1:443".parse().unwrap())
        .tun_active());
}

#[tokio::test]
async fn unexpected_runtime_cleanup_failure_retains_egress_and_error() {
    let config =
        zero_config::RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#)
            .unwrap();
    let proxy = crate::Proxy::new(config).unwrap();
    let physical = zero_platform_tokio::EgressInterface::new("ethernet", 9).unwrap();
    proxy
        .egress_interface
        .replace_for(false, Some(physical.clone()));
    proxy
        .egress_interface
        .replace_tunnel_addresses(["10.66.0.1".parse().unwrap()]);
    let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let (_done_tx, done) = tokio::sync::oneshot::channel();
    let (route_done_tx, route_done) = tokio::sync::oneshot::channel();
    *proxy.tun_control.lock().unwrap() = Some(crate::runtime::TunControl {
        id: 42,
        shutdown,
        done,
        route_done: Some(route_done),
    });

    let finalize = super::finalize_tun_runtime_exit(&proxy, 42);
    let reject_cleanup = async move {
        shutdown_rx.changed().await.unwrap();
        route_done_tx
            .send(Err("remove stale capture route".to_owned()))
            .unwrap();
    };
    tokio::join!(finalize, reject_cleanup);

    assert_eq!(proxy.egress_interface.current_for(false), Some(physical));
    assert!(proxy
        .tun_last_error
        .lock()
        .unwrap()
        .as_deref()
        .unwrap()
        .contains("remove stale capture route"));
}

#[cfg(all(feature = "udp-runtime", feature = "dns"))]
#[tokio::test]
async fn command_tun_start_rejects_fake_ip_pool_collision_before_device_creation() {
    let config = zero_config::RuntimeConfig::parse(
        r#"{
            "runtime": {
                "dns": {
                    "servers": { "global": { "type": "udp", "host": "1.1.1.1" } },
                    "default_server": "global",
                    "answer": { "type": "fake_ip" }
                }
            },
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse fake-IP config");
    let proxy = crate::Proxy::new(config).expect("build proxy");
    let error = proxy
        .start_tun(
            super::TunInterfaceOptions {
                name: Some("zero-collision-test"),
                addr: "198.18.0.1/24",
                mask: "255.255.255.0",
                secondary_addr: None,
            },
            1500,
            "tun-test",
            super::TunRuntimeOptions {
                auto_route: false,
                include_cidrs: Vec::new(),
                exclude_cidrs: Vec::new(),
                dual_stack: false,
                strict_route: true,
                dns_hijack: true,
            },
        )
        .await
        .expect_err("collision must fail before opening a TUN device");

    assert!(error.to_string().contains("overlaps TUN-owned address"));
}

fn configured_tun_fixture() -> (zero_config::TunConfig, TunInfo) {
    let config = zero_config::TunConfig {
        name: Some("zero-test".to_owned()),
        addr: "10.66.0.1/24".to_owned(),
        mask: "255.255.255.0".to_owned(),
        secondary_addr: None,
        mtu: None,
        tag: "tun-in".to_owned(),
        auto_route: true,
        include_cidrs: Vec::new(),
        exclude_cidrs: Vec::new(),
        dual_stack: false,
        strict_route: true,
        dns_hijack: true,
    };
    let route_exclusions = vec![IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10))];
    let info = TunInfo {
        id: 1,
        name: "zero-test".to_owned(),
        addr: config.addr.clone(),
        addresses: vec![config.addr.clone()],
        mtu: 1500,
        tag: config.tag.clone(),
        auto_route: true,
        include_cidrs: Vec::new(),
        exclude_cidrs: Vec::new(),
        dual_stack: false,
        strict_route: true,
        dns_hijack: true,
        dns_hijacked_queries: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        healthy: true,
        last_error: None,
        egress_interface: None,
        egress_interface_v4: None,
        egress_interface_v6: None,
        route_exclusions,
        managed_config: Some(config.clone()),
    };
    (config, info)
}

#[test]
fn tun_address_accepts_cidr_and_derives_mask() {
    let (address, mask) =
        parse_address_and_mask("10.10.0.1/24", "255.255.255.0").expect("parse IPv4 CIDR");
    assert_eq!(address, IpAddr::V4(Ipv4Addr::new(10, 10, 0, 1)));
    assert_eq!(mask, IpAddr::V4(Ipv4Addr::new(255, 255, 255, 0)));

    let (address, mask) = parse_address_and_mask("fd00::1/64", "::").expect("parse IPv6 CIDR");
    assert_eq!(address, IpAddr::V6("fd00::1".parse::<Ipv6Addr>().unwrap()));
    assert_eq!(
        mask,
        IpAddr::V6("ffff:ffff:ffff:ffff::".parse::<Ipv6Addr>().unwrap())
    );
}

#[test]
fn tun_address_rejects_invalid_prefix_and_mixed_mask_family() {
    assert!(parse_address_and_mask("10.0.0.1/33", "255.255.255.0").is_err());
    assert!(parse_address_and_mask("10.0.0.1", "ffff:ffff::").is_err());
    let mismatch = parse_address_and_mask("10.0.0.1/24", "255.255.255.252")
        .expect_err("CIDR prefix and explicit mask must agree");
    assert!(mismatch.to_string().contains("different networks"));
}

#[test]
fn automatic_routes_are_dual_stack_by_default() {
    let addresses = parse_interface_addresses("10.0.0.1/24", "255.255.255.0", None, true).unwrap();
    assert_eq!(addresses.len(), 2);
    assert_eq!(addresses[0].cidr, "10.0.0.1/24");
    assert_eq!(addresses[1].cidr, DEFAULT_TUN_IPV6_ADDR);

    let addresses = parse_interface_addresses("fd00::1/64", "::", None, true).unwrap();
    assert_eq!(addresses[1].cidr, DEFAULT_TUN_IPV4_ADDR);

    let addresses = parse_interface_addresses("fd00::1/64", "::", None, false).unwrap();
    assert_eq!(addresses.len(), 1);
}

#[test]
fn explicit_secondary_address_must_be_cidr_and_opposite_family() {
    let addresses =
        parse_interface_addresses("10.0.0.1/24", "255.255.255.0", Some("fd77::1/64"), true)
            .unwrap();
    assert_eq!(addresses[1].cidr, "fd77::1/64");
    assert!(
        parse_interface_addresses("10.0.0.1/24", "255.255.255.0", Some("fd77::1"), true).is_err()
    );
    assert!(
        parse_interface_addresses("10.0.0.1/24", "255.255.255.0", Some("10.1.0.1/24"), true)
            .is_err()
    );
}

#[test]
fn strict_dns_hijack_discovers_and_merges_system_dns_endpoints() {
    let system = zero_config::RuntimeConfig::parse(
        r#"{
            "runtime":{"dns":{
                "servers":{
                    "local":{"type":"system"},
                    "fallback":{"type":"udp","host":"198.51.100.53"}
                },
                "default_server":"local"
            }},
            "route":{"rules":[],"final":{"type":"direct"}}
        }"#,
    )
    .expect("parse system DNS config");
    assert_eq!(
        configured_dns_endpoint_addresses_with(&system, || {
            Ok(vec![
                "192.0.2.53".parse().unwrap(),
                "192.0.2.53".parse().unwrap(),
            ])
        })
        .expect("discover system DNS endpoints"),
        vec![
            "192.0.2.53".parse::<IpAddr>().unwrap(),
            "198.51.100.53".parse().unwrap(),
        ]
    );

    let udp = zero_config::RuntimeConfig::parse(
        r#"{
            "runtime":{"dns":{
                "servers":{"global":{"type":"udp","host":"1.1.1.1"}},
                "default_server":"global"
            }},
            "route":{"rules":[],"final":{"type":"direct"}}
        }"#,
    )
    .expect("parse UDP DNS config");
    assert_eq!(
        configured_dns_endpoint_addresses(&udp).expect("literal DNS endpoint"),
        vec![IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))]
    );
}

#[test]
fn strict_dns_hijack_fails_closed_when_system_dns_discovery_fails() {
    let system = zero_config::RuntimeConfig::parse(
        r#"{
            "runtime":{"dns":{
                "servers":{"local":{"type":"system"}},
                "default_server":"local"
            }},
            "route":{"rules":[],"final":{"type":"direct"}}
        }"#,
    )
    .expect("parse system DNS config");
    let error = configured_dns_endpoint_addresses_with(&system, || {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "no upstream endpoints",
        ))
    })
    .expect_err("strict system DNS must fail closed");
    assert!(error.to_string().contains("no upstream endpoints"));
}

#[test]
fn doh_endpoint_parser_accepts_literal_v4_and_v6_hosts() {
    let config = |host: &str| {
        zero_config::RuntimeConfig::parse(&format!(
            r#"{{
                "runtime":{{"dns":{{
                    "servers":{{"global":{{"type":"doh","host":"{host}"}}}},
                    "default_server":"global"
                }}}},
                "route":{{"rules":[],"final":{{"type":"direct"}}}}
            }}"#
        ))
        .expect("parse DoH config")
    };
    assert_eq!(
        configured_dns_endpoint_addresses(&config("1.1.1.1")).unwrap(),
        vec![IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))]
    );
    assert_eq!(
        configured_dns_endpoint_addresses(&config("2606:4700:4700::1111")).unwrap(),
        vec!["2606:4700:4700::1111".parse::<IpAddr>().unwrap()]
    );
    let domain = zero_config::RuntimeConfig::parse(
        r#"{
            "runtime":{"dns":{
                "servers":{"global":{"type":"doh","host":"dns.example"}},
                "default_server":"global"
            }},
            "route":{"rules":[],"final":{"type":"direct"}}
        }"#,
    );
    assert!(matches!(
        domain,
        Err(zero_config::ConfigError::InvalidDns(_))
    ));
}

#[test]
fn configured_tun_restarts_when_explicit_route_exclusions_change() {
    let (config, info) = configured_tun_fixture();
    let unchanged = PreparedTunNetwork {
        dns_hijack: true,
        route_exclusions: info.route_exclusions.clone(),
    };
    assert!(configured_tun_is_current(&info, &config, 1500, &unchanged));

    let changed = PreparedTunNetwork {
        dns_hijack: true,
        route_exclusions: vec![IpAddr::V4(Ipv4Addr::new(192, 0, 2, 11))],
    };
    assert!(!configured_tun_is_current(&info, &config, 1500, &changed));
}

#[test]
fn loopback_and_link_local_bootstrap_addresses_do_not_get_physical_host_routes() {
    for address in [
        "0.0.0.0",
        "127.0.0.1",
        "169.254.10.20",
        "224.0.0.1",
        "255.255.255.255",
        "::",
        "::1",
        "fe80::1",
        "ff02::1",
    ] {
        assert!(!tun_route_exclusion_required(address.parse().unwrap()));
    }
    assert!(tun_route_exclusion_required("1.1.1.1".parse().unwrap()));
    assert!(tun_route_exclusion_required(
        "2606:4700:4700::1111".parse().unwrap()
    ));
}
