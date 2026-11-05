//! A cold native source does not authorize OS work or block RFC 6761 answers.
use crate::{native::NativeDnsSource, DnsQueryRole, DnsSystem};
use std::{
    io,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use zero_traits::{AddressFamily, DnsResolver, IpAddress};

fn cold() -> (DnsSystem, Arc<NativeDnsSource>, Arc<NativeDnsSource>) {
    let query = Arc::new(NativeDnsSource::default());
    let node = Arc::new(NativeDnsSource::default());
    let dns = DnsSystem::build(None)
        .unwrap()
        .with_native_query_queries(query.clone())
        .with_native_node_queries(node.clone());
    (dns, query, node)
}
fn ipv4() -> IpAddress {
    IpAddress::V4([127, 0, 0, 1])
}
fn ipv6() -> IpAddress {
    IpAddress::V6(std::net::Ipv6Addr::LOCALHOST.octets())
}

#[tokio::test]
async fn cold_configured_system_localhost_preserves_policy_and_dedicated_node_lifecycle() {
    let query = Arc::new(NativeDnsSource::default());
    let node = Arc::new(NativeDnsSource::default());
    let config = zero_config::DnsConfig {
        servers: std::collections::BTreeMap::from([(
            "system".into(),
            zero_config::DnsServerConfig::System,
        )]),
        default_server: "system".into(),
        dispatch: vec![],
        cache: None,
        reverse_mapping: None,
        answer: zero_config::DnsAnswerConfig::Real,
        policy: zero_config::DnsPolicyConfig {
            node_server: Some("system".into()),
            address_family: zero_config::DnsAddressFamilyPolicy::PreferIpv6,
            ..Default::default()
        },
    };
    let dns = DnsSystem::build(Some(&config))
        .unwrap()
        .with_native_query_queries(query.clone())
        .with_native_node_queries(node.clone());
    assert_eq!(
        dns.resolve_real("localhost").await.unwrap(),
        vec![ipv6(), ipv4()]
    );
    assert_eq!(
        dns.resolve_node("localhost").await.unwrap(),
        vec![ipv6(), ipv4()]
    );
    assert_eq!(
        dns.resolve_direct_with_family("localhost", AddressFamily::OnlyIpv4)
            .await
            .unwrap(),
        vec![ipv4()]
    );
    assert_eq!(
        dns.resolve_real("requires-native.invalid")
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotConnected
    );
    assert!(dns.native_query_observations().is_empty());
    assert!(query.current_budget().is_none() && node.current_budget().is_none());
    dns.shutdown_queries(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(
        dns.resolve_node("localhost").await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
}

#[tokio::test]
async fn cold_system_localhost_preserves_dual_stack_and_never_admits_native_work() {
    let (dns, query, node) = cold();
    for name in ["localhost", "LOCALHOST.", "deep.app.localhost"] {
        assert_eq!(
            DnsResolver::resolve(&dns, name).await.unwrap(),
            vec![ipv4(), ipv6()]
        );
        assert_eq!(dns.resolve_real(name).await.unwrap(), vec![ipv4(), ipv6()]);
        assert_eq!(dns.resolve_real_type(name, 1).await.unwrap(), vec![ipv4()]);
        assert_eq!(dns.resolve_real_type(name, 28).await.unwrap(), vec![ipv6()]);
        assert_eq!(
            dns.resolve_direct_with_family(name, AddressFamily::OnlyIpv4)
                .await
                .unwrap(),
            vec![ipv4()]
        );
        assert_eq!(
            dns.resolve_direct_with_family(name, AddressFamily::OnlyIpv6)
                .await
                .unwrap(),
            vec![ipv6()]
        );
        assert_eq!(dns.resolve_node(name).await.unwrap(), vec![ipv4(), ipv6()]);
    }
    assert!(dns.native_query_observations().is_empty());
    assert!(query.current_budget().is_none() && node.current_budget().is_none());
    assert!(query.tasks().is_err() && node.tasks().is_err());
    assert_eq!(
        dns.resolve_real("requires-native.invalid")
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotConnected
    );
}

#[tokio::test]
async fn cold_system_localhost_still_rejects_closed_coordinators_and_stale_generations() {
    let (dns, query, node) = cold();
    let old = dns.config_generation.load(Ordering::Acquire);
    dns.reload(None).unwrap();
    assert_ne!(old, dns.config_generation.load(Ordering::Acquire));
    assert_eq!(
        dns.resolve_system_coordinated("localhost", DnsQueryRole::Default, old)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::Interrupted
    );
    assert_eq!(
        dns.resolve_system_type_with_family_coordinated(
            "localhost",
            1,
            DnsQueryRole::Direct,
            AddressFamily::OnlyIpv4,
            old
        )
        .await
        .unwrap_err()
        .kind(),
        io::ErrorKind::Interrupted
    );
    assert_eq!(
        dns.resolve_real("localhost").await.unwrap(),
        vec![ipv4(), ipv6()]
    );
    dns.shutdown_queries(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    for name in ["localhost", "deep.app.localhost"] {
        assert_eq!(
            DnsResolver::resolve(&dns, name).await.unwrap_err().kind(),
            io::ErrorKind::NotConnected
        );
        assert_eq!(
            dns.resolve_real(name).await.unwrap_err().kind(),
            io::ErrorKind::NotConnected
        );
        assert_eq!(
            dns.resolve_real_type(name, 28).await.unwrap_err().kind(),
            io::ErrorKind::NotConnected
        );
        assert_eq!(
            dns.resolve_node(name).await.unwrap_err().kind(),
            io::ErrorKind::NotConnected
        );
    }
    assert!(dns.native_query_observations().is_empty());
    assert!(query.current_budget().is_none() && node.current_budget().is_none());
}
