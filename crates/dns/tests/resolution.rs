use std::collections::BTreeMap;

use zero_config::{DnsAnswerConfig, DnsConfig, DnsServerConfig};
use zero_dns::DnsSystem;
use zero_traits::{DnsResolver, IpAddress};

#[tokio::test]
async fn real_resolution_bypasses_fake_ip_allocation() {
    let config = DnsConfig {
        servers: BTreeMap::from([("system".to_owned(), DnsServerConfig::System)]),
        default_server: "system".to_owned(),
        dispatch: Vec::new(),
        cache: None,
        answer: DnsAnswerConfig::FakeIp {
            cidr: "198.18.0.0/15".to_owned(),
            ttl_seconds: 60,
            exclude_domains: Vec::new(),
        },
    };
    let dns = DnsSystem::build(Some(&config)).expect("build DNS system");

    let synthetic = dns.resolve("localhost").await.expect("allocate fake IP");
    assert!(synthetic.iter().all(is_benchmark_ip));

    let real = dns
        .resolve_real("localhost")
        .await
        .expect("resolve localhost through the real backend");
    assert!(real.iter().any(is_loopback));
    assert!(real.iter().all(|address| !is_benchmark_ip(address)));
}

fn is_benchmark_ip(address: &IpAddress) -> bool {
    matches!(address, IpAddress::V4([198, octet, _, _]) if *octet == 18 || *octet == 19)
}

fn is_loopback(address: &IpAddress) -> bool {
    match address {
        IpAddress::V4([127, _, _, _]) => true,
        IpAddress::V6(bytes) => *bytes == std::net::Ipv6Addr::LOCALHOST.octets(),
        _ => false,
    }
}
