#![cfg(feature = "udp")]

use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_config::{
    DnsAddressFamilyPolicy, DnsAnswerConfig, DnsCacheConfig, DnsConfig, DnsPolicyConfig,
    DnsServerConfig,
};
use zero_traits::IpAddress;

fn query(domain: &str, query_type: u16, edns_size: Option<u16>) -> Vec<u8> {
    let mut query = vec![
        0x42,
        0x17,
        0x01,
        0x00,
        0x00,
        0x01,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        u8::from(edns_size.is_some()),
    ];
    for label in domain.split('.') {
        query.push(label.len() as u8);
        query.extend_from_slice(label.as_bytes());
    }
    query.push(0);
    query.extend_from_slice(&query_type.to_be_bytes());
    query.extend_from_slice(&1_u16.to_be_bytes());
    if let Some(size) = edns_size {
        query.push(0);
        query.extend_from_slice(&41_u16.to_be_bytes());
        query.extend_from_slice(&size.to_be_bytes());
        query.extend_from_slice(&0_u32.to_be_bytes());
        query.extend_from_slice(&0_u16.to_be_bytes());
    }
    query
}

fn response_header(query: &[u8], answer_count: u16, rcode: u8, truncated: bool) -> Vec<u8> {
    zero_dns::udp::parse_dns_question(query).expect("parse question");
    let question_end = question_end(query);
    let mut response = Vec::new();
    response.extend_from_slice(&query[..2]);
    response.push(0x81 | if truncated { 0x02 } else { 0 });
    response.push(0x80 | rcode);
    response.extend_from_slice(&1_u16.to_be_bytes());
    response.extend_from_slice(&answer_count.to_be_bytes());
    response.extend_from_slice(&0_u16.to_be_bytes());
    response.extend_from_slice(&0_u16.to_be_bytes());
    response.extend_from_slice(&query[12..question_end]);
    response
}

fn question_end(message: &[u8]) -> usize {
    let mut offset = 12;
    while message[offset] != 0 {
        offset += usize::from(message[offset]) + 1;
    }
    offset + 5
}

fn append_record(response: &mut Vec<u8>, record_type: u16, ttl: u32, data: &[u8]) {
    response.extend_from_slice(&[0xc0, 0x0c]);
    response.extend_from_slice(&record_type.to_be_bytes());
    response.extend_from_slice(&1_u16.to_be_bytes());
    response.extend_from_slice(&ttl.to_be_bytes());
    response.extend_from_slice(&(data.len() as u16).to_be_bytes());
    response.extend_from_slice(data);
}

fn config(port: u16, answer: DnsAnswerConfig) -> DnsConfig {
    DnsConfig {
        servers: BTreeMap::from([(
            "local".to_owned(),
            DnsServerConfig::Udp {
                host: "127.0.0.1".to_owned(),
                port,
                bootstrap: Vec::new(),
            },
        )]),
        default_server: "local".to_owned(),
        dispatch: Vec::new(),
        cache: None,
        answer,
        policy: Default::default(),
    }
}

#[tokio::test]
async fn address_resolution_queries_a_and_aaaa_independently() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind DNS");
    let port = socket.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let mut queries = Vec::new();
        for _ in 0..2 {
            let mut request = [0_u8; 4096];
            let (size, peer) = socket.recv_from(&mut request).await.unwrap();
            queries.push((request[..size].to_vec(), peer));
        }
        for (request, peer) in queries {
            let question = zero_dns::udp::parse_dns_question(&request).unwrap();
            let address = match question.query_type {
                1 => IpAddress::V4([192, 0, 2, 9]),
                28 => IpAddress::V6(Ipv4Addr::new(192, 0, 2, 9).to_ipv6_mapped().octets()),
                other => panic!("unexpected query type {other}"),
            };
            let response = zero_dns::udp::build_dns_response(&request, &[address]);
            socket.send_to(&response, peer).await.unwrap();
        }
    });
    let dns = zero_dns::DnsSystem::build(Some(&config(port, DnsAnswerConfig::Real))).unwrap();

    let addresses = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        dns.resolve_real("dual.example"),
    )
    .await
    .expect("A and AAAA lookups should run concurrently")
    .unwrap();

    assert_eq!(addresses.len(), 2);
    assert!(matches!(addresses[0], IpAddress::V4(_)));
    assert!(matches!(addresses[1], IpAddress::V6(_)));
    server.await.unwrap();
}

#[tokio::test]
async fn dns_policy_times_out_primary_and_uses_explicit_fallback() {
    let primary = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind primary DNS");
    let primary_port = primary.local_addr().unwrap().port();
    let primary_task = tokio::spawn(async move {
        let mut request = [0_u8; 4096];
        let _ = primary.recv_from(&mut request).await.unwrap();
    });

    let fallback = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind fallback DNS");
    let fallback_port = fallback.local_addr().unwrap().port();
    let fallback_task = tokio::spawn(async move {
        let mut request = [0_u8; 4096];
        let (size, peer) = fallback.recv_from(&mut request).await.unwrap();
        let response =
            zero_dns::udp::build_dns_response(&request[..size], &[IpAddress::V4([192, 0, 2, 44])]);
        fallback.send_to(&response, peer).await.unwrap();
    });

    let dns = zero_dns::DnsSystem::build(Some(&DnsConfig {
        servers: BTreeMap::from([
            (
                "primary".to_owned(),
                DnsServerConfig::Udp {
                    host: "127.0.0.1".to_owned(),
                    port: primary_port,
                    bootstrap: Vec::new(),
                },
            ),
            (
                "fallback".to_owned(),
                DnsServerConfig::Udp {
                    host: "127.0.0.1".to_owned(),
                    port: fallback_port,
                    bootstrap: Vec::new(),
                },
            ),
        ]),
        default_server: "primary".to_owned(),
        dispatch: Vec::new(),
        cache: None,
        answer: DnsAnswerConfig::Real,
        policy: DnsPolicyConfig {
            timeout_ms: 50,
            fallback_servers: vec!["fallback".to_owned()],
            address_family: DnsAddressFamilyPolicy::Ipv4Only,
        },
    }))
    .unwrap();

    let addresses = dns.resolve_real("fallback.example").await.unwrap();
    assert_eq!(addresses, vec![IpAddress::V4([192, 0, 2, 44])]);
    primary_task.await.unwrap();
    fallback_task.await.unwrap();
}

#[tokio::test]
async fn dns_address_family_policy_controls_queries_and_result_order() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind DNS");
    let port = socket.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let mut request = [0_u8; 4096];
            let (size, peer) = socket.recv_from(&mut request).await.unwrap();
            let question = zero_dns::udp::parse_dns_question(&request[..size]).unwrap();
            let address = match question.query_type {
                1 => IpAddress::V4([192, 0, 2, 55]),
                28 => IpAddress::V6([0x20, 1, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 55]),
                other => panic!("unexpected query type {other}"),
            };
            let response = zero_dns::udp::build_dns_response(&request[..size], &[address]);
            socket.send_to(&response, peer).await.unwrap();
        }
    });
    let mut config = config(port, DnsAnswerConfig::Real);
    config.policy.address_family = DnsAddressFamilyPolicy::PreferIpv6;
    let dns = zero_dns::DnsSystem::build(Some(&config)).unwrap();

    let addresses = dns.resolve_real("prefer-v6.example").await.unwrap();
    assert!(matches!(addresses[0], IpAddress::V6(_)));
    assert!(matches!(addresses[1], IpAddress::V4(_)));
    server.await.unwrap();
}

#[tokio::test]
async fn fake_ip_returns_a_and_explicit_aaaa_nodata() {
    let dns = zero_dns::DnsSystem::build(Some(&DnsConfig {
        servers: BTreeMap::from([("system".to_owned(), DnsServerConfig::System)]),
        default_server: "system".to_owned(),
        dispatch: Vec::new(),
        cache: None,
        answer: DnsAnswerConfig::FakeIp {
            cidr: "198.18.0.0/15".to_owned(),
            ipv6_cidr: None,
            ttl_seconds: 90,
            max_entries: Some(32),
            exclude_domains: Vec::new(),
        },
        policy: Default::default(),
    }))
    .unwrap();

    let a = dns
        .answer_udp_query(&query("fake.example", 1, None))
        .await
        .unwrap();
    let aaaa = dns
        .answer_udp_query(&query("fake.example", 28, None))
        .await
        .unwrap();

    assert_eq!(u16::from_be_bytes([a[6], a[7]]), 1);
    assert_eq!(u16::from_be_bytes([aaaa[6], aaaa[7]]), 0);
    assert_eq!(aaaa[3] & 0x0f, 0);
}

#[tokio::test]
async fn dual_stack_fake_ip_answers_aaaa_and_supports_reverse_lookup() {
    let dns = zero_dns::DnsSystem::build(Some(&DnsConfig {
        servers: BTreeMap::from([("system".to_owned(), DnsServerConfig::System)]),
        default_server: "system".to_owned(),
        dispatch: Vec::new(),
        cache: None,
        answer: DnsAnswerConfig::FakeIp {
            cidr: "198.18.0.0/15".to_owned(),
            ipv6_cidr: Some("fd00::/120".to_owned()),
            ttl_seconds: 90,
            max_entries: Some(32),
            exclude_domains: Vec::new(),
        },
        policy: Default::default(),
    }))
    .unwrap();

    let response = dns
        .answer_udp_query(&query("dual-fake.example", 28, None))
        .await
        .unwrap();
    assert_eq!(u16::from_be_bytes([response[6], response[7]]), 1);
    let address_offset = response.len() - 16;
    let mut octets = [0_u8; 16];
    octets.copy_from_slice(&response[address_offset..]);
    assert_eq!(
        dns.lookup_fake_ip(&IpAddress::V6(octets)).await.as_deref(),
        Some("dual-fake.example")
    );
    assert_eq!(dns.fake_ip_stats().await.unwrap().live_mappings, 1);
}

#[tokio::test]
async fn non_address_records_and_rcodes_are_forwarded_unchanged() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = socket.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        for index in 0..2 {
            let mut request = [0_u8; 4096];
            let (size, peer) = socket.recv_from(&mut request).await.unwrap();
            let request = &request[..size];
            let response = if index == 0 {
                let mut response = response_header(request, 1, 0, false);
                append_record(&mut response, 5, 123, &[5, b'a', b'l', b'i', b'a', b's', 0]);
                response
            } else {
                response_header(request, 0, 3, false)
            };
            socket.send_to(&response, peer).await.unwrap();
        }
    });
    let dns = zero_dns::DnsSystem::build(Some(&config(port, DnsAnswerConfig::Real))).unwrap();

    let cname_query = query("cname.example", 5, Some(1232));
    let cname = dns.answer_udp_query(&cname_query).await.unwrap();
    let nxdomain_query = query("missing.example", 16, None);
    let nxdomain = dns.answer_udp_query(&nxdomain_query).await.unwrap();

    assert_eq!(u16::from_be_bytes([cname[6], cname[7]]), 1);
    assert!(cname
        .windows(7)
        .any(|bytes| bytes == [5, b'a', b'l', b'i', b'a', b's', 0]));
    assert_eq!(nxdomain[3] & 0x0f, 3);
    server.await.unwrap();
}

#[tokio::test]
async fn oversized_udp_response_is_truncated_but_tcp_answer_is_complete() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = socket.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let mut request = [0_u8; 4096];
            let (size, peer) = socket.recv_from(&mut request).await.unwrap();
            let request = &request[..size];
            let mut response = response_header(request, 4, 0, false);
            let data = vec![b'x'; 250];
            for _ in 0..4 {
                append_record(&mut response, 16, 300, &data);
            }
            socket.send_to(&response, peer).await.unwrap();
        }
    });
    let dns = zero_dns::DnsSystem::build(Some(&config(port, DnsAnswerConfig::Real))).unwrap();
    let request = query("large.example", 16, None);

    let udp = dns.answer_udp_query(&request).await.unwrap();
    let tcp = dns.answer_tcp_query(&request).await.unwrap();

    assert!(udp.len() <= 512);
    assert_ne!(udp[2] & 0x02, 0);
    assert!(tcp.len() > 1000);
    assert_eq!(tcp[2] & 0x02, 0);
    server.await.unwrap();
}

#[tokio::test]
async fn truncated_upstream_udp_response_falls_back_to_tcp() {
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = tcp.local_addr().unwrap().port();
    let udp = tokio::net::UdpSocket::bind(("127.0.0.1", port))
        .await
        .unwrap();
    let udp_server = tokio::spawn(async move {
        let mut request = [0_u8; 4096];
        let (size, peer) = udp.recv_from(&mut request).await.unwrap();
        let response = response_header(&request[..size], 0, 0, true);
        udp.send_to(&response, peer).await.unwrap();
    });
    let tcp_server = tokio::spawn(async move {
        let (mut stream, _) = tcp.accept().await.unwrap();
        let size = stream.read_u16().await.unwrap() as usize;
        let mut request = vec![0_u8; size];
        stream.read_exact(&mut request).await.unwrap();
        let response =
            zero_dns::udp::build_dns_response(&request, &[IpAddress::V4([203, 0, 113, 44])]);
        stream.write_u16(response.len() as u16).await.unwrap();
        stream.write_all(&response).await.unwrap();
    });
    let dns = zero_dns::DnsSystem::build(Some(&config(port, DnsAnswerConfig::Real))).unwrap();

    let addresses = dns.resolve_real_type("fallback.example", 1).await.unwrap();

    assert_eq!(addresses, vec![IpAddress::V4([203, 0, 113, 44])]);
    udp_server.await.unwrap();
    tcp_server.await.unwrap();
}

#[tokio::test]
async fn mismatched_transaction_id_is_ignored() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = socket.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let mut request = [0_u8; 4096];
        let (size, peer) = socket.recv_from(&mut request).await.unwrap();
        let request = &request[..size];
        let mut wrong =
            zero_dns::udp::build_dns_response(request, &[IpAddress::V4([192, 0, 2, 1])]);
        wrong[1] ^= 1;
        socket.send_to(&wrong, peer).await.unwrap();
        let valid = zero_dns::udp::build_dns_response(request, &[IpAddress::V4([192, 0, 2, 2])]);
        socket.send_to(&valid, peer).await.unwrap();
    });
    let dns = zero_dns::DnsSystem::build(Some(&config(port, DnsAnswerConfig::Real))).unwrap();

    assert_eq!(
        dns.resolve_real_type("id.example", 1).await.unwrap(),
        vec![IpAddress::V4([192, 0, 2, 2])]
    );
    server.await.unwrap();
}

#[tokio::test]
async fn raw_response_cache_preserves_records_and_rewrites_transaction_id() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = socket.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let mut request = [0_u8; 4096];
        let (size, peer) = socket.recv_from(&mut request).await.unwrap();
        let mut response = response_header(&request[..size], 1, 0, false);
        append_record(&mut response, 65, 120, &[0, 1, 0, 0, 0, 0]);
        socket.send_to(&response, peer).await.unwrap();
    });
    let mut config = config(port, DnsAnswerConfig::Real);
    config.cache = Some(DnsCacheConfig {
        max_entries: 8,
        max_ttl_seconds: Some(60),
    });
    let dns = zero_dns::DnsSystem::build(Some(&config)).unwrap();
    let first_query = query("https.example", 65, Some(1232));
    let mut second_query = first_query.clone();
    second_query[..2].copy_from_slice(&0x9911_u16.to_be_bytes());

    let first = dns.answer_udp_query(&first_query).await.unwrap();
    let second = dns.answer_udp_query(&second_query).await.unwrap();

    assert_eq!(&first[..2], &first_query[..2]);
    assert_eq!(&second[..2], &second_query[..2]);
    assert_eq!(&first[2..], &second[2..]);
    server.await.unwrap();
}

#[tokio::test]
async fn fake_ip_exclusion_forwards_real_a_and_aaaa() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = socket.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let mut request = [0_u8; 4096];
            let (size, peer) = socket.recv_from(&mut request).await.unwrap();
            let request = &request[..size];
            let question = zero_dns::udp::parse_dns_question(request).unwrap();
            let address = if question.query_type == 1 {
                IpAddress::V4([203, 0, 113, 8])
            } else {
                IpAddress::V6([0x20, 1, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8])
            };
            let response = zero_dns::udp::build_dns_response(request, &[address]);
            socket.send_to(&response, peer).await.unwrap();
        }
    });
    let dns = zero_dns::DnsSystem::build(Some(&config(
        port,
        DnsAnswerConfig::FakeIp {
            cidr: "198.18.0.0/15".to_owned(),
            ipv6_cidr: None,
            ttl_seconds: 60,
            max_entries: Some(16),
            exclude_domains: vec!["*.real.example".to_owned()],
        },
    )))
    .unwrap();

    let a = dns
        .answer_udp_query(&query("api.real.example", 1, None))
        .await
        .unwrap();
    let aaaa = dns
        .answer_udp_query(&query("api.real.example", 28, None))
        .await
        .unwrap();

    assert_eq!(&a[a.len() - 4..], &[203, 0, 113, 8]);
    assert_eq!(u16::from_be_bytes([aaaa[6], aaaa[7]]), 1);
    assert!(dns
        .lookup_fake_ip_domain("api.real.example")
        .await
        .is_none());
    server.await.unwrap();
}
