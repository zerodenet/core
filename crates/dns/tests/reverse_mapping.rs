#![cfg(feature = "udp")]

use std::collections::BTreeMap;

use zero_config::{
    DnsAddressFamilyPolicy, DnsAnswerConfig, DnsConfig, DnsPolicyConfig, DnsReverseMappingConfig,
    DnsServerConfig,
};
use zero_dns::RealIpReverseLookup;
use zero_traits::{DnsResolver, IpAddress};

fn config(port: u16) -> DnsConfig {
    DnsConfig {
        servers: BTreeMap::from([(
            "local".to_owned(),
            DnsServerConfig::Udp {
                host: "127.0.0.1".to_owned(),
                port,
                bootstrap: Vec::new(),
                detour: None,
            },
        )]),
        default_server: "local".to_owned(),
        dispatch: Vec::new(),
        cache: None,
        reverse_mapping: Some(DnsReverseMappingConfig {
            max_entries: 16,
            max_domains_per_address: 4,
            max_ttl_seconds: 60,
        }),
        answer: DnsAnswerConfig::Real,
        policy: DnsPolicyConfig {
            timeout_ms: 1_000,
            fallback_servers: Vec::new(),
            address_family: DnsAddressFamilyPolicy::Ipv4Only,
            ..Default::default()
        },
    }
}

#[tokio::test]
async fn records_real_answers_and_preserves_compatible_reload() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind DNS");
    let port = socket.local_addr().expect("DNS local address").port();
    let server = tokio::spawn(async move {
        let mut request = [0_u8; 4096];
        let (size, peer) = socket.recv_from(&mut request).await.expect("receive query");
        let response =
            zero_dns::udp::build_dns_response(&request[..size], &[IpAddress::V4([192, 0, 2, 44])]);
        socket
            .send_to(&response, peer)
            .await
            .expect("send response");
    });

    let config = config(port);
    let dns = zero_dns::DnsSystem::build(Some(&config)).expect("build DNS");
    assert_eq!(
        dns.resolve("Real.Example.").await.expect("resolve real IP"),
        vec![IpAddress::V4([192, 0, 2, 44])]
    );
    assert_eq!(
        dns.lookup_real_ip(&IpAddress::V4([192, 0, 2, 44])).await,
        RealIpReverseLookup::Resolved("real.example".to_owned())
    );

    dns.reload(Some(&config)).expect("compatible reload");
    assert_eq!(
        dns.lookup_real_ip(&IpAddress::V4([192, 0, 2, 44])).await,
        RealIpReverseLookup::Resolved("real.example".to_owned())
    );
    server.await.expect("DNS server task");
}

#[tokio::test]
async fn refuses_to_guess_shared_real_ip_domain() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind DNS");
    let port = socket.local_addr().expect("DNS local address").port();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let mut request = [0_u8; 4096];
            let (size, peer) = socket.recv_from(&mut request).await.expect("receive query");
            let response = zero_dns::udp::build_dns_response(
                &request[..size],
                &[IpAddress::V4([192, 0, 2, 55])],
            );
            socket
                .send_to(&response, peer)
                .await
                .expect("send response");
        }
    });

    let dns = zero_dns::DnsSystem::build(Some(&config(port))).expect("build DNS");
    dns.resolve("one.example").await.expect("first lookup");
    dns.resolve("two.example").await.expect("second lookup");
    assert_eq!(
        dns.lookup_real_ip(&IpAddress::V4([192, 0, 2, 55])).await,
        RealIpReverseLookup::Ambiguous
    );
    server.await.expect("DNS server task");
}
