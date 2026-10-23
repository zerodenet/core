use std::collections::BTreeMap;

use zero_config::{DnsAddressFamilyPolicy, DnsAnswerConfig, DnsConfig, DnsServerConfig};
use zero_dns::{DnsQueryRole, DnsSystem};
use zero_traits::{AddressFamily, DnsResolver, IpAddress};

#[path = "localhost/persistence.rs"]
mod persistence;

const V4: IpAddress = IpAddress::V4([127, 0, 0, 1]);
const V6: IpAddress = IpAddress::V6([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
const NAMES: [&str; 4] = [
    "localhost",
    "LOCALHOST.",
    "app.localhost",
    "deep.App.Localhost.",
];

#[derive(Debug)]
struct NoUpstream;

impl zero_dns::DnsOutboundConnector for NoUpstream {
    fn connect(&self, _: String, _: std::net::SocketAddr) -> zero_dns::DnsOutboundConnectFuture {
        panic!("localhost must not open an upstream DNS connection")
    }

    fn exchange_datagram(
        &self,
        _: String,
        _: std::net::SocketAddr,
        _: Vec<u8>,
    ) -> zero_dns::DnsOutboundDatagramFuture {
        panic!("localhost must not send upstream DNS datagrams")
    }
}

fn config(fake: bool, family: DnsAddressFamilyPolicy) -> DnsConfig {
    #[cfg(feature = "udp")]
    let backend = DnsServerConfig::Udp {
        host: "127.0.0.1".into(),
        port: 9,
        bootstrap: Vec::new(),
        detour: Some("must-not-connect".into()),
    };
    #[cfg(not(feature = "udp"))]
    let backend = DnsServerConfig::System;
    DnsConfig {
        servers: BTreeMap::from([("unavailable".into(), backend)]),
        default_server: "unavailable".into(),
        dispatch: Vec::new(),
        cache: None,
        reverse_mapping: None,
        answer: if fake {
            DnsAnswerConfig::FakeIp {
                cidr: "198.18.0.0/24".into(),
                ipv6_cidr: None,
                ttl_seconds: 3600,
                max_entries: Some(16),
                exclude_domains: Vec::new(),
            }
        } else {
            DnsAnswerConfig::Real
        },
        policy: zero_config::DnsPolicyConfig {
            address_family: family,
            timeout_ms: 20,
            ..Default::default()
        },
    }
}

fn build(config: &DnsConfig) -> DnsSystem {
    let dns = DnsSystem::build(Some(config)).unwrap();
    dns.set_outbound_connector(std::sync::Arc::new(NoUpstream));
    dns
}

fn query(domain: &str, kind: u16) -> Vec<u8> {
    let mut packet = vec![0x12, 0x34, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0];
    for label in domain.trim_end_matches('.').split('.') {
        packet.push(label.len() as u8);
        packet.extend_from_slice(label.as_bytes());
    }
    packet.push(0);
    packet.extend_from_slice(&kind.to_be_bytes());
    packet.extend_from_slice(&1_u16.to_be_bytes());
    packet
}

#[tokio::test]
async fn localhost_service_preserves_family_generation_and_query_retirement() {
    use zero_platform_tokio::contract::dns::{DnsQueryContext, DnsRequest, DnsService};

    let dns = build(&config(true, DnsAddressFamilyPolicy::PreferIpv4));
    let generation = dns.generation();
    for role in [
        DnsQueryRole::Default,
        DnsQueryRole::Direct,
        DnsQueryRole::Node,
        DnsQueryRole::Bootstrap,
    ] {
        let result = DnsService::resolve(
            &dns,
            DnsRequest {
                name: "APP.Localhost.".into(),
                role,
                family: AddressFamily::OnlyIpv6,
            },
            DnsQueryContext {
                expected_generation: Some(generation),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(result.addresses, vec![V6]);
        assert_eq!(result.generation, generation);
    }
    assert_eq!(dns.fake_ip_stats().await.unwrap().live_mappings, 0);
    dns.shutdown_queries(tokio::time::Instant::now() + std::time::Duration::from_secs(1))
        .await
        .unwrap();
    assert!(dns.resolve_real("app.localhost").await.is_err());
}

#[tokio::test]
async fn localhost_names_bypass_fake_ip_and_all_resolution_backends() {
    for fake in [false, true] {
        let dns = build(&config(fake, DnsAddressFamilyPolicy::PreferIpv4));
        for name in NAMES {
            for addresses in [
                dns.resolve(name).await,
                dns.resolve_real(name).await,
                dns.resolve_direct(name).await,
                dns.resolve_node(name).await,
            ] {
                assert_eq!(addresses.unwrap(), vec![V4, V6], "{name}");
            }
            assert_eq!(dns.resolve_real_type(name, 1).await.unwrap(), vec![V4]);
            assert_eq!(dns.resolve_real_type(name, 28).await.unwrap(), vec![V6]);
            assert_eq!(dns.resolve_direct_ipv4(name).await.unwrap(), vec![V4]);
            assert_eq!(dns.lookup_fake_ip_domain(name).await, None);
            for role in [
                DnsQueryRole::Default,
                DnsQueryRole::Direct,
                DnsQueryRole::Node,
            ] {
                assert!(dns.recent_query_attempts(name, role, 8).is_empty());
            }
        }
        if fake {
            assert_eq!(dns.fake_ip_stats().await.unwrap().live_mappings, 0);
        }
    }
}

#[tokio::test]
async fn localhost_wire_answers_are_local_for_udp_tcp_and_non_address_types() {
    for fake in [false, true] {
        let dns = build(&config(fake, DnsAddressFamilyPolicy::PreferIpv4));
        for name in NAMES {
            for kind in [1, 28, 16, 33, 64, 65] {
                let query = query(name, kind);
                for response in [
                    dns.answer_udp_query(&query).await.unwrap(),
                    dns.answer_tcp_query(&query).await.unwrap(),
                ] {
                    assert_eq!(&response[..2], &query[..2]);
                    assert_eq!(response[3] & 15, 0, "localhost negative answers are NODATA");
                    assert_eq!(
                        u16::from_be_bytes([response[6], response[7]]),
                        u16::from(matches!(kind, 1 | 28))
                    );
                    match kind {
                        1 => assert!(response.ends_with(&[127, 0, 0, 1])),
                        28 => assert!(response.ends_with(&std::net::Ipv6Addr::LOCALHOST.octets())),
                        _ => assert_eq!(response.len(), query.len()),
                    }
                }
            }
        }
        if fake {
            assert_eq!(dns.fake_ip_stats().await.unwrap().live_mappings, 0);
        }
    }
}

#[tokio::test]
async fn localhost_preserves_family_policy_and_strict_direct_overrides() {
    for (family, expected) in [
        (DnsAddressFamilyPolicy::Ipv4Only, vec![V4]),
        (DnsAddressFamilyPolicy::Ipv6Only, vec![V6]),
        (DnsAddressFamilyPolicy::PreferIpv4, vec![V4, V6]),
        (DnsAddressFamilyPolicy::PreferIpv6, vec![V6, V4]),
    ] {
        let dns = build(&config(true, family));
        assert_eq!(dns.resolve("app.localhost").await.unwrap(), expected);
        assert_eq!(
            dns.resolve_direct_with_family("app.localhost", AddressFamily::OnlyIpv4)
                .await
                .unwrap(),
            vec![V4]
        );
        assert_eq!(
            dns.resolve_direct_with_family("app.localhost", AddressFamily::OnlyIpv6)
                .await
                .unwrap(),
            vec![V6]
        );
        for (kind, ip) in [(1, V4), (28, V6)] {
            let answer = dns
                .answer_udp_query(&query("localhost", kind))
                .await
                .unwrap();
            assert_eq!(
                u16::from_be_bytes([answer[6], answer[7]]),
                u16::from(expected.contains(&ip))
            );
        }
    }
}

#[tokio::test]
async fn localhost_system_fallback_is_independent_of_os_hosts_and_dns() {
    let dns = DnsSystem::build(None).unwrap();
    for name in NAMES {
        assert_eq!(dns.resolve(name).await.unwrap(), vec![V4, V6]);
        assert_eq!(dns.resolve_node(name).await.unwrap(), vec![V4, V6]);
        assert_eq!(dns.resolve_real_type(name, 28).await.unwrap(), vec![V6]);
        assert_eq!(
            dns.resolve_direct_with_family(name, AddressFamily::OnlyIpv4)
                .await
                .unwrap(),
            vec![V4]
        );
        assert_eq!(
            dns.resolve_direct_with_family(name, AddressFamily::OnlyIpv6)
                .await
                .unwrap(),
            vec![V6]
        );
    }
    let wire = dns
        .answer_tcp_query(&query("app.localhost", 65))
        .await
        .unwrap();
    assert_eq!(wire[3] & 15, 0);
    assert_eq!(&wire[6..8], &[0, 0]);
}

#[tokio::test]
async fn localhost_ech_queries_do_not_contact_the_explicit_upstream() {
    let dns = build(&config(true, DnsAddressFamilyPolicy::PreferIpv4));
    for name in NAMES {
        let answer = dns
            .query_ech_config(name, "https://127.0.0.1:9/dns-query")
            .await
            .unwrap();
        assert_eq!(answer.config_list, None);
    }
}

#[tokio::test]
async fn localhost_label_boundaries_leave_unrelated_fake_ip_names_unchanged() {
    let dns = build(&config(true, DnsAddressFamilyPolicy::PreferIpv4));
    for name in ["notlocalhost", "localhost.example", "app.localhost.example"] {
        let ips = dns.resolve(name).await.unwrap();
        assert!(
            matches!(ips.as_slice(), [IpAddress::V4([198, 18, 0, _])]),
            "{name}: {ips:?}"
        );
        assert!(dns.lookup_fake_ip_domain(name).await.is_some());
    }
    assert_eq!(dns.fake_ip_stats().await.unwrap().live_mappings, 3);
}
