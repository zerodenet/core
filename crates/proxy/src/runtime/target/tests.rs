use std::collections::BTreeMap;

use zero_config::{
    DnsAddressFamilyPolicy, DnsAnswerConfig, DnsConfig, DnsPolicyConfig, DnsReverseMappingConfig,
    DnsServerConfig,
};
use zero_core::{Address, Network, ProtocolType, Session, TargetHostSource};
use zero_traits::{DnsResolver, IpAddress};

use super::resolve_dns_target;

#[tokio::test]
async fn recovers_only_transparent_real_ip_targets_and_preserves_direct_ip() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind DNS");
    let port = socket.local_addr().expect("DNS local address").port();
    let server = tokio::spawn(async move {
        let mut request = [0_u8; 4096];
        let (size, peer) = socket.recv_from(&mut request).await.expect("receive query");
        let response =
            zero_dns::udp::build_dns_response(&request[..size], &[IpAddress::V4([192, 0, 2, 80])]);
        socket
            .send_to(&response, peer)
            .await
            .expect("send response");
    });
    let dns = zero_dns::DnsSystem::build(Some(&DnsConfig {
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
    }))
    .expect("build DNS");
    dns.resolve("transparent.example")
        .await
        .expect("populate reverse index");

    let original = Address::Ipv4([192, 0, 2, 80]);
    let mut explicit = Session::new(
        1,
        original.clone(),
        443,
        Network::Tcp,
        ProtocolType::UNKNOWN,
    );
    resolve_dns_target(&dns, &mut explicit).await;
    assert_eq!(explicit.target, original);

    let mut transparent = Session::new(
        2,
        original.clone(),
        443,
        Network::Tcp,
        ProtocolType::UNKNOWN,
    );
    transparent.transparent_target = true;
    resolve_dns_target(&dns, &mut transparent).await;
    assert_eq!(
        transparent.target,
        Address::Domain("transparent.example".to_owned())
    );
    assert_eq!(transparent.direct_target, Some(original.clone()));
    assert_eq!(transparent.original_target, Some(original));
    assert_eq!(
        transparent.target_host_source,
        Some(TargetHostSource::DnsReverse)
    );
    server.await.expect("DNS server task");
}
