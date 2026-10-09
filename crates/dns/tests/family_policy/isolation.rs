use std::io;
use std::sync::Arc;

use zero_config::DnsAddressFamilyPolicy;
use zero_dns::DnsSystem;
use zero_traits::{AddressFamily, IpAddress};

use super::support::{Fixture, V4, V6};

#[tokio::test]
async fn positive_cache_isolates_auto_ipv4_ipv6_and_egress_generation() {
    let server = Fixture::new(vec![V4, V6], false).await;
    let egress = zero_platform_tokio::EgressInterfaceControl::default();
    let dns = DnsSystem::build_with_egress(Some(&server.config(DnsAddressFamilyPolicy::Ipv4Only)), egress.clone()).unwrap();
    for _ in 0..2 {
        for (family, expected) in [(AddressFamily::Auto, V4), (AddressFamily::OnlyIpv4, V4), (AddressFamily::OnlyIpv6, V6)] {
            assert_eq!(dns.resolve_direct_with_family("cache.test", family).await.unwrap(), vec![expected]);
        }
    }
    assert_eq!(server.queries(), vec![1, 1, 28]);
    egress.invalidate_network();
    assert_eq!(dns.resolve_direct_with_family("cache.test", AddressFamily::OnlyIpv4).await.unwrap(), vec![V4]);
    assert_eq!(server.queries(), vec![1, 1, 28, 1]);
}

#[tokio::test]
async fn negative_cache_does_not_cross_family_policy_or_reload() {
    let server = Fixture::new(Vec::new(), false).await;
    let config = server.config(DnsAddressFamilyPolicy::Ipv4Only);
    let dns = DnsSystem::build(Some(&config)).unwrap();
    assert_eq!(dns.resolve_direct_with_family("negative.test", AddressFamily::OnlyIpv4).await.unwrap_err().kind(), io::ErrorKind::NotFound);
    server.answer(vec![V4, V6]);
    assert_eq!(dns.resolve_direct_with_family("negative.test", AddressFamily::Auto).await.unwrap(), vec![V4]);
    assert_eq!(dns.resolve_direct_with_family("negative.test", AddressFamily::OnlyIpv6).await.unwrap(), vec![V6]);
    assert_eq!(dns.resolve_direct_with_family("negative.test", AddressFamily::OnlyIpv4).await.unwrap_err().kind(), io::ErrorKind::NotFound);
    assert_eq!(server.queries(), vec![1, 1, 28]);
    dns.reload(Some(&config)).unwrap();
    assert_eq!(dns.resolve_direct_with_family("negative.test", AddressFamily::OnlyIpv4).await.unwrap(), vec![V4]);
    assert_eq!(server.queries(), vec![1, 1, 28, 1]);
}

#[tokio::test]
async fn simultaneous_policies_do_not_share_a_policy_evaluated_flight() {
    let server = Fixture::new(vec![V4, V6], true).await;
    let dns = Arc::new(DnsSystem::build(Some(&server.config(DnsAddressFamilyPolicy::Ipv4Only))).unwrap());
    let mut tasks = Vec::new();
    for family in [AddressFamily::Auto, AddressFamily::OnlyIpv4, AddressFamily::OnlyIpv6] {
        let dns = dns.clone();
        tasks.push(tokio::spawn(async move {
            dns.resolve_direct_with_family("flight.test", family).await
        }));
    }
    server.wait_for_queries(3).await;
    server.release();
    for (task, expected) in tasks.into_iter().zip([V4, V4, V6]) {
        assert_eq!(task.await.unwrap().unwrap(), vec![expected]);
    }
    let mut queries = server.queries();
    queries.sort_unstable();
    assert_eq!(queries, vec![1, 1, 28]);
}

#[tokio::test]
async fn reload_does_not_join_an_old_flight_or_reuse_its_success() {
    let old = Fixture::new(vec![V4], true).await;
    let replacement = IpAddress::V4([198, 51, 100, 7]);
    let current = Fixture::new(vec![replacement], false).await;
    let dns = Arc::new(DnsSystem::build(Some(&old.config(DnsAddressFamilyPolicy::Ipv4Only))).unwrap());
    let old_lookup = {
        let dns = dns.clone();
        tokio::spawn(async move { dns.resolve_direct_with_family("reload.test", AddressFamily::OnlyIpv4).await })
    };
    old.wait_for_queries(1).await;
    dns.reload(Some(&current.config(DnsAddressFamilyPolicy::Ipv4Only))).unwrap();
    assert_eq!(dns.resolve_direct_with_family("reload.test", AddressFamily::OnlyIpv4).await.unwrap(), vec![replacement]);
    old.release();
    assert_eq!(old_lookup.await.unwrap().unwrap(), vec![V4]);
    assert_eq!(dns.resolve_direct_with_family("reload.test", AddressFamily::OnlyIpv4).await.unwrap(), vec![replacement]);
    assert_eq!(current.queries(), vec![1]);
}
