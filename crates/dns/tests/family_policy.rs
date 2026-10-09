#![cfg(feature = "udp")]

use std::io;

use zero_config::DnsAddressFamilyPolicy;
use zero_dns::DnsSystem;
use zero_traits::{AddressFamily, IpAddress};

#[path = "family_policy/support.rs"]
mod support;
#[path = "family_policy/isolation.rs"]
mod isolation;
use support::{Fixture, V4, V6};

#[tokio::test]
async fn strict_direct_family_overrides_global_but_keeps_selected_resolver() {
    let default = Fixture::new(vec![V4, V6], false).await;
    let direct = Fixture::new(vec![V4, V6], false).await;
    let mut config = default.config(DnsAddressFamilyPolicy::Ipv6Only);
    config.servers.insert("direct".into(), direct.server());
    config.policy.direct_server = Some("direct".into());
    let dns = DnsSystem::build(Some(&config)).unwrap();

    assert_eq!(dns.resolve_direct_with_family("override.test", AddressFamily::OnlyIpv4).await.unwrap(), vec![V4]);
    assert_eq!(direct.queries(), vec![1], "only IPv4 must not issue AAAA");
    assert!(default.queries().is_empty(), "business policy changed resolver selection");
    assert_eq!(dns.resolve_direct_with_family("override.test", AddressFamily::Auto).await.unwrap(), vec![V6]);
    assert_eq!(direct.queries(), vec![1, 28]);
    assert_eq!(dns.resolve_node("override.test").await.unwrap(), vec![V6]);
    assert_eq!(default.queries(), vec![28], "node resolution inherited a business policy");
}

#[tokio::test]
async fn only_ipv6_overrides_global_ipv4_and_preserves_dns_fallback_chain() {
    let fallback = Fixture::new(vec![V4, V6], false).await;
    let failed = Fixture::new(vec![V4, V6], true).await;
    let mut config = failed.config(DnsAddressFamilyPolicy::Ipv4Only);
    config.servers.insert("fallback".into(), fallback.server());
    config.policy.direct_server = Some("fixture".into());
    config.policy.direct_fallback_servers = vec!["fallback".into()];
    config.policy.timeout_ms = 50;
    let dns = DnsSystem::build(Some(&config)).unwrap();

    assert_eq!(dns.resolve_direct_with_family("fallback.test", AddressFamily::OnlyIpv6).await.unwrap(), vec![V6]);
    assert_eq!(failed.queries(), vec![28]);
    assert_eq!(fallback.queries(), vec![28], "only IPv6 fallback must not query A");
}

#[tokio::test]
async fn only_ipv6_rejects_ipv4_mapped_aaaa_candidates() {
    let mapped = IpAddress::V6("::ffff:192.0.2.42".parse::<std::net::Ipv6Addr>().unwrap().octets());
    let server = Fixture::new(vec![mapped], false).await;
    let dns = DnsSystem::build(Some(&server.config(DnsAddressFamilyPolicy::PreferIpv6))).unwrap();
    assert_eq!(dns.resolve_direct_with_family("mapped.test", AddressFamily::OnlyIpv6).await.unwrap_err().kind(), io::ErrorKind::NotFound);
    assert_eq!(dns.resolve_direct("mapped.test").await.unwrap(), vec![V4]);
}
