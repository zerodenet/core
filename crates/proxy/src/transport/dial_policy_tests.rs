use super::DirectConnector;
use std::sync::Arc;
use zero_core::{Address, Network, ProtocolType, Session};
use zero_traits::{AddressFamily, DialPolicy};
fn session(target: Address, port: u16) -> Session {
    Session::new(1, target, port, Network::Tcp, ProtocolType::UNKNOWN)
}
#[tokio::test]
async fn direct_family_filters_literals_and_normalizes_mapped_addresses() {
    let resolver = zero_dns::DnsSystem::build(None).unwrap();
    let egress = Default::default();
    let mapped: std::net::Ipv6Addr = "::ffff:127.0.0.1".parse().unwrap();
    let target = session(Address::Ipv6(mapped.octets()), 443);
    let policy = DialPolicy {
        address_family: AddressFamily::OnlyIpv4,
        ..Default::default()
    };
    let resolution = DirectConnector
        .resolve_target_addrs_with_policy(&target, &resolver, &egress, &policy)
        .await
        .unwrap();
    assert_eq!(resolution.candidates, ["127.0.0.1:443".parse().unwrap()]);
    let policy = DialPolicy {
        address_family: AddressFamily::OnlyIpv6,
        ..Default::default()
    };
    assert!(DirectConnector
        .resolve_target_addrs_with_policy(&target, &resolver, &egress, &policy)
        .await
        .is_err());
}
#[tokio::test]
async fn direct_tcp_binds_source_and_observes_actual_local_address() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let resolver = Arc::new(zero_dns::DnsSystem::build(None).unwrap());
    let policy = DialPolicy {
        source_ip: Some(endpoint.ip()),
        ..Default::default()
    };
    let connection = DirectConnector
        .connect_with_policy(
            &session(Address::Ipv4([127, 0, 0, 1]), endpoint.port()),
            &resolver,
            &Default::default(),
            &policy,
        )
        .await
        .unwrap_or_else(|f| panic!("{}: {}", f.stage, f.error));
    let (_, peer) = listener.accept().await.unwrap();
    assert_eq!(peer.ip(), endpoint.ip());
    assert_eq!(
        connection.network.local_address.unwrap().host,
        endpoint.ip().to_string()
    );
    assert_eq!(
        connection.network.address_family_policy.as_deref(),
        Some("only_ipv4")
    );
}
#[tokio::test]
async fn source_family_prevents_wrong_family_connect_before_socket() {
    let resolver = Arc::new(zero_dns::DnsSystem::build(None).unwrap());
    let policy = DialPolicy {
        source_ip: Some("127.0.0.1".parse().unwrap()),
        ..Default::default()
    };
    let failure = DirectConnector
        .connect_with_policy(
            &session(Address::Ipv6(std::net::Ipv6Addr::LOCALHOST.octets()), 443),
            &resolver,
            &Default::default(),
            &policy,
        )
        .await
        .err()
        .expect("opposite family must fail");
    assert_eq!(failure.stage, "resolve_direct_target");
    assert!(failure.network.connection_attempts.is_empty());
}
#[tokio::test]
async fn tun_recovery_may_use_ipv4_only_when_the_leaf_allows_it() {
    let resolver = zero_dns::DnsSystem::build(None).unwrap();
    let egress = zero_platform_tokio::EgressInterfaceControl::default();
    egress.mark_unavailable_for(true, "no IPv6 underlay");
    egress.replace_tunnel_addresses(["10.66.0.1".parse().unwrap(), "fd66::1".parse().unwrap()]);
    let mut target = session(Address::Domain("localhost".into()), 443);
    target.direct_target = Some(Address::Ipv6(
        "2001:db8::1"
            .parse::<std::net::Ipv6Addr>()
            .unwrap()
            .octets(),
    ));
    target.target_host_source = Some(zero_core::TargetHostSource::TlsSni);
    for family in [AddressFamily::OnlyIpv4, AddressFamily::OnlyIpv6] {
        let policy = DialPolicy {
            address_family: family,
            ..Default::default()
        };
        let resolution = DirectConnector
            .resolve_target_addrs_with_policy(&target, &resolver, &egress, &policy)
            .await
            .unwrap();
        assert!(!resolution.candidates.is_empty());
        assert!(resolution
            .candidates
            .iter()
            .all(|a| policy.allows_ip(a.ip())));
        if family == AddressFamily::OnlyIpv6 {
            assert!(egress
                .select_for_peer_with_policy(resolution.candidates[0], &policy)
                .is_err());
        }
    }
}
