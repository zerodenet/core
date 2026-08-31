use std::collections::BTreeMap;

use zero_config::{
    DnsAddressFamilyPolicy, DnsAnswerConfig, DnsConfig, DnsPolicyConfig, DnsReverseMappingConfig,
    DnsServerConfig,
};
use zero_core::{Address, FakeIpReverseStatus, Network, ProtocolType, Session, TargetHostSource};
use zero_engine::EngineError;
use zero_traits::{DnsResolver, IpAddress};

use super::resolve_dns_target;

#[cfg(feature = "dns")]
mod fake_ipv6_fallback;

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
    resolve_dns_target(&dns, &mut explicit)
        .await
        .expect("preserve explicit target");
    assert_eq!(explicit.target, original);

    let mut transparent = Session::new(
        2,
        original.clone(),
        443,
        Network::Tcp,
        ProtocolType::UNKNOWN,
    );
    transparent.transparent_target = true;
    resolve_dns_target(&dns, &mut transparent)
        .await
        .expect("recover transparent target");
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

#[tokio::test]
async fn missing_ipv4_and_ipv6_fake_ip_mappings_fail_closed() {
    let dns = fake_dns();
    for (target, address) in [
        (Address::Ipv4([198, 18, 0, 7]), "198.18.0.7"),
        (
            Address::Ipv6([0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7]),
            "fd00::7",
        ),
    ] {
        let mut session = Session::new(1, target.clone(), 443, Network::Tcp, ProtocolType::UNKNOWN);
        session.transparent_target = true;

        let error = resolve_dns_target(&dns, &mut session)
            .await
            .expect_err("unmapped synthetic target must fail");

        assert!(matches!(
            error,
            EngineError::FakeIpReverseMissing { address: ref value } if value == address
        ));
        assert_eq!(session.target, target.clone());
        assert_eq!(session.original_target, Some(target));
        assert!(session.direct_target.is_none());
        assert!(session.target_host_source.is_none());
        assert_eq!(
            session.fake_ip_reverse_status,
            Some(FakeIpReverseStatus::Missing)
        );
    }
}

#[tokio::test]
async fn sniffed_tcp_and_quic_domains_cannot_bypass_fake_ip_ownership() {
    let dns = fake_dns();
    let synthetic = Address::Ipv4([198, 18, 0, 9]);
    for source in [
        TargetHostSource::TlsSni,
        TargetHostSource::HttpHost,
        TargetHostSource::QuicSni,
    ] {
        let network = if source == TargetHostSource::QuicSni {
            Network::Udp
        } else {
            Network::Tcp
        };
        let mut session = Session::new(
            1,
            Address::Domain("sniffed.example".to_owned()),
            443,
            network,
            ProtocolType::UNKNOWN,
        );
        session.transparent_target = true;
        session.original_target = Some(synthetic.clone());
        session.direct_target = Some(synthetic.clone());
        session.target_host_source = Some(source);

        let error = resolve_dns_target(&dns, &mut session)
            .await
            .expect_err("sniffing must not repair a missing Fake-IP mapping");

        assert_eq!(error.code(), "fake_ip_reverse_missing");
        assert_eq!(session.target, synthetic);
        assert!(session.direct_target.is_none());
        assert!(session.target_host_source.is_none());
        assert_eq!(
            session.fake_ip_reverse_status,
            Some(FakeIpReverseStatus::Missing)
        );
    }
}

#[tokio::test]
async fn live_fake_ip_mapping_is_authoritative_over_sniffed_domain() {
    let dns = fake_dns();
    let response = dns
        .answer_udp_query(&dns_a_query("mapped.example"))
        .await
        .expect("allocate Fake-IP mapping");
    let synthetic = Address::Ipv4(
        response[response.len() - 4..]
            .try_into()
            .expect("four-byte A response"),
    );
    let mut session = Session::new(
        1,
        Address::Domain("different-sni.example".to_owned()),
        443,
        Network::Tcp,
        ProtocolType::UNKNOWN,
    );
    session.transparent_target = true;
    session.original_target = Some(synthetic.clone());
    session.direct_target = Some(synthetic.clone());
    session.target_host_source = Some(TargetHostSource::TlsSni);
    session.sni = Some("different-sni.example".to_owned());

    resolve_dns_target(&dns, &mut session)
        .await
        .expect("restore mapped Fake-IP domain");

    assert_eq!(session.target, Address::Domain("mapped.example".to_owned()));
    assert_eq!(session.original_target, Some(synthetic));
    assert!(session.direct_target.is_none());
    assert_eq!(session.target_host_source, Some(TargetHostSource::FakeIp));
    assert_eq!(
        session.fake_ip_reverse_status,
        Some(FakeIpReverseStatus::Resolved)
    );
    assert_eq!(session.sni.as_deref(), Some("different-sni.example"));
}

fn fake_dns() -> zero_dns::DnsSystem {
    zero_dns::DnsSystem::build(Some(&DnsConfig {
        servers: BTreeMap::from([("system".to_owned(), DnsServerConfig::System)]),
        default_server: "system".to_owned(),
        dispatch: Vec::new(),
        cache: None,
        reverse_mapping: None,
        answer: DnsAnswerConfig::FakeIp {
            cidr: "198.18.0.0/24".to_owned(),
            ipv6_cidr: Some("fd00::/120".to_owned()),
            ttl_seconds: 60,
            max_entries: Some(16),
            exclude_domains: Vec::new(),
        },
        policy: DnsPolicyConfig::default(),
    }))
    .expect("build Fake-IP DNS")
}

fn dns_a_query(domain: &str) -> Vec<u8> {
    let mut query = vec![
        0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    for label in domain.split('.') {
        query.push(label.len() as u8);
        query.extend_from_slice(label.as_bytes());
    }
    query.push(0);
    query.extend_from_slice(&1_u16.to_be_bytes());
    query.extend_from_slice(&1_u16.to_be_bytes());
    query
}
