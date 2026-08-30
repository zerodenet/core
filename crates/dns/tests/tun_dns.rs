use std::collections::BTreeMap;

use zero_config::{DnsAnswerConfig, DnsConfig, DnsServerConfig};

fn query(domain: &str, query_type: u16) -> Vec<u8> {
    let mut query = vec![
        0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
    ];
    for label in domain.split('.') {
        query.push(label.len() as u8);
        query.extend_from_slice(label.as_bytes());
    }
    query.push(0);
    query.extend_from_slice(&query_type.to_be_bytes());
    query.extend_from_slice(&1_u16.to_be_bytes());
    // EDNS OPT is deliberately present; the response must clear ARCOUNT when
    // it does not copy the additional section.
    query.extend_from_slice(&[0, 0, 41, 4, 208, 0, 0, 0, 0, 0, 0]);
    query
}

#[tokio::test]
async fn tun_query_uses_fake_ip_and_returns_a_well_formed_header() {
    let dns = zero_dns::DnsSystem::build(Some(&DnsConfig {
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
    }))
    .expect("build DNS");
    let response = dns
        .answer_udp_query(&query("webrtc.example", 1))
        .await
        .expect("answer query");

    assert_eq!(&response[..2], &[0x12, 0x34]);
    assert_eq!(u16::from_be_bytes([response[6], response[7]]), 1);
    assert_eq!(u16::from_be_bytes([response[10], response[11]]), 0);
    assert_eq!(
        dns.lookup_fake_ip_domain("webrtc.example").await.as_deref(),
        Some("198.18.0.1")
    );
}

#[tokio::test]
async fn compatible_reload_preserves_live_fake_ip_mapping() {
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
            max_entries: Some(16),
            exclude_domains: Vec::new(),
        },
        policy: Default::default(),
    };
    let dns = zero_dns::DnsSystem::build(Some(&config)).expect("build DNS");
    dns.answer_udp_query(&query("reload.example", 1))
        .await
        .expect("allocate mapping");
    let before = dns
        .lookup_fake_ip_domain("reload.example")
        .await
        .expect("mapping before reload");

    dns.reload(Some(&config)).expect("reload DNS");

    assert_eq!(
        dns.lookup_fake_ip_domain("RELOAD.EXAMPLE.")
            .await
            .as_deref(),
        Some(before.as_str())
    );
    assert_eq!(dns.fake_ip_stats().await.unwrap().live_mappings, 1);
}

#[tokio::test]
async fn retired_pool_exhaustion_returns_servfail_without_real_dns_fallback() {
    let dns = zero_dns::DnsSystem::build(Some(&DnsConfig {
        servers: BTreeMap::from([("system".to_owned(), DnsServerConfig::System)]),
        default_server: "system".to_owned(),
        dispatch: Vec::new(),
        cache: None,
        reverse_mapping: None,
        answer: DnsAnswerConfig::FakeIp {
            cidr: "198.18.0.0/30".to_owned(),
            ipv6_cidr: None,
            ttl_seconds: 60,
            max_entries: Some(1),
            exclude_domains: Vec::new(),
        },
        policy: Default::default(),
    }))
    .expect("build DNS");

    dns.answer_udp_query(&query("retired.example", 1))
        .await
        .expect("allocate first address");
    dns.clear_fake_ip(zero_dns::FakeIpClearTarget::Domain(
        "retired.example".to_owned(),
    ))
    .await
    .expect("retire first address")
    .expect("Fake-IP enabled");
    dns.answer_udp_query(&query("live.example", 1))
        .await
        .expect("allocate second address");

    let exhausted = dns
        .answer_udp_query(&query("blocked.example", 1))
        .await
        .expect("build SERVFAIL response");
    assert_eq!(exhausted[3] & 0x0f, 2, "pool exhaustion must be SERVFAIL");
    assert_eq!(u16::from_be_bytes([exhausted[6], exhausted[7]]), 0);
    assert_eq!(
        dns.lookup_fake_ip_domain("live.example").await.as_deref(),
        Some("198.18.0.2"),
        "failed allocation must preserve the last live mapping"
    );

    let error = zero_traits::DnsResolver::resolve(&dns, "internal.example")
        .await
        .expect_err("internal resolver must not fall back to a real address");
    assert_eq!(error.kind(), std::io::ErrorKind::AddrNotAvailable);
    assert!(error.to_string().contains("live or retired"));
}

#[tokio::test]
async fn compatible_reload_preserves_retired_fake_ip_address() {
    let config = DnsConfig {
        servers: BTreeMap::from([("system".to_owned(), DnsServerConfig::System)]),
        default_server: "system".to_owned(),
        dispatch: Vec::new(),
        cache: None,
        reverse_mapping: None,
        answer: DnsAnswerConfig::FakeIp {
            cidr: "198.18.0.0/30".to_owned(),
            ipv6_cidr: None,
            ttl_seconds: 60,
            max_entries: Some(1),
            exclude_domains: Vec::new(),
        },
        policy: Default::default(),
    };
    let dns = zero_dns::DnsSystem::build(Some(&config)).expect("build DNS");
    dns.answer_udp_query(&query("removed.example", 1))
        .await
        .expect("allocate removed mapping");
    dns.clear_fake_ip(zero_dns::FakeIpClearTarget::All)
        .await
        .expect("clear Fake-IP")
        .expect("Fake-IP enabled");

    dns.reload(Some(&config)).expect("compatible reload");

    let response = dns
        .answer_udp_query(&query("replacement.example", 1))
        .await
        .expect("allocate replacement mapping");
    assert_eq!(u16::from_be_bytes([response[6], response[7]]), 1);
    assert_eq!(
        dns.lookup_fake_ip_domain("replacement.example")
            .await
            .as_deref(),
        Some("198.18.0.2")
    );
    assert_eq!(
        dns.fake_ip_stats()
            .await
            .expect("Fake-IP stats")
            .retired_addresses,
        1
    );
}

#[test]
fn parser_rejects_multiple_questions() {
    let mut request = query("example.com", 1);
    request[5] = 2;
    assert!(zero_dns::udp::parse_dns_question(&request).is_err());
}
