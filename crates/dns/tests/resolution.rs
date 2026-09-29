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
        reverse_mapping: None,
        answer: DnsAnswerConfig::FakeIp {
            cidr: "198.18.0.0/15".to_owned(),
            ipv6_cidr: None,
            ttl_seconds: 60,
            max_entries: None,
            exclude_domains: Vec::new(),
        },
        policy: Default::default(),
    };
    let dns = DnsSystem::build(Some(&config)).expect("build DNS system");

    // The host may itself use an intercepted/Fake-IP DNS resolver. A numeric
    // loopback keeps this System-backend test independent of /etc/hosts and DNS.
    let synthetic = dns.resolve("127.0.0.1").await.expect("allocate fake IP");
    assert!(synthetic.iter().all(is_benchmark_ip));

    let real = dns
        .resolve_real("127.0.0.1")
        .await
        .expect("resolve loopback through the real backend");
    assert!(real.iter().any(is_loopback));
    assert!(real.iter().all(|address| !is_benchmark_ip(address)));
}

#[cfg(feature = "udp")]
#[tokio::test]
async fn real_domain_resolution_queries_the_backend_instead_of_fake_ip_allocator() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind DNS fixture");
    let port = socket.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let mut query = [0; 4096];
        let (size, peer) = socket.recv_from(&mut query).await.unwrap();
        let response =
            zero_dns::udp::build_dns_response(&query[..size], &[IpAddress::V4([203, 0, 113, 7])]);
        socket.send_to(&response, peer).await.unwrap();
    });
    let config = DnsConfig {
        servers: BTreeMap::from([(
            "fixture".to_owned(),
            DnsServerConfig::Udp {
                host: "127.0.0.1".to_owned(),
                port,
                bootstrap: Vec::new(),
                detour: None,
            },
        )]),
        default_server: "fixture".to_owned(),
        dispatch: Vec::new(),
        cache: None,
        reverse_mapping: None,
        answer: DnsAnswerConfig::FakeIp {
            cidr: "198.18.0.0/15".to_owned(),
            ipv6_cidr: None,
            ttl_seconds: 60,
            max_entries: None,
            exclude_domains: Vec::new(),
        },
        policy: zero_config::DnsPolicyConfig {
            timeout_ms: 1000,
            address_family: zero_config::DnsAddressFamilyPolicy::Ipv4Only,
            ..Default::default()
        },
    };
    let dns = DnsSystem::build(Some(&config)).unwrap();
    let synthetic = dns.resolve("resolution.test").await.unwrap();
    assert!(synthetic.iter().all(is_benchmark_ip));
    let real = dns.resolve_real("resolution.test").await.unwrap();
    assert_eq!(real, vec![IpAddress::V4([203, 0, 113, 7])]);
    assert_eq!(dns.resolve("resolution.test").await.unwrap(), synthetic);
    server.await.unwrap();
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
