use std::collections::BTreeMap;
use std::io;

use zero_config::{DnsAnswerConfig, DnsConfig, DnsPolicyConfig, DnsServerConfig};
use zero_traits::IpAddress;

pub(super) const TYPE_A: u16 = 1;
pub(super) const TYPE_CNAME: u16 = 5;
pub(super) const TYPE_AAAA: u16 = 28;

pub(super) async fn resolve_once(
    domain: &str,
    query_type: u16,
    build_response: impl FnOnce(&[u8]) -> Vec<u8> + Send + 'static,
) -> io::Result<Vec<IpAddress>> {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind instrumented DNS server");
    let port = socket.local_addr().expect("DNS endpoint").port();
    let server = tokio::spawn(async move {
        let mut request = [0_u8; 4096];
        let (size, peer) = socket
            .recv_from(&mut request)
            .await
            .expect("receive DNS query");
        let response = build_response(&request[..size]);
        socket
            .send_to(&response, peer)
            .await
            .expect("send DNS response");
    });

    let dns = zero_dns::DnsSystem::build(Some(&config(port))).expect("build DNS system");
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        dns.resolve_real_type(domain, query_type),
    )
    .await
    .expect("DNS resolution timed out");
    server.await.expect("instrumented DNS server task");
    result
}

pub(super) fn config(port: u16) -> DnsConfig {
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
        reverse_mapping: None,
        answer: DnsAnswerConfig::Real,
        policy: DnsPolicyConfig {
            timeout_ms: 250,
            ..Default::default()
        },
    }
}

pub(super) fn response_header(query: &[u8], answer_count: u16) -> Vec<u8> {
    zero_dns::udp::parse_dns_question(query).expect("parse DNS question");
    let question_end = question_end(query);
    let mut response = Vec::new();
    response.extend_from_slice(&query[..2]);
    response.extend_from_slice(&[0x81, 0x80]);
    response.extend_from_slice(&1_u16.to_be_bytes());
    response.extend_from_slice(&answer_count.to_be_bytes());
    response.extend_from_slice(&0_u16.to_be_bytes());
    response.extend_from_slice(&0_u16.to_be_bytes());
    response.extend_from_slice(&query[12..question_end]);
    response
}

pub(super) fn append_record(response: &mut Vec<u8>, owner: &str, kind: u16, ttl: u32, data: &[u8]) {
    encode_name(owner, response);
    response.extend_from_slice(&kind.to_be_bytes());
    response.extend_from_slice(&1_u16.to_be_bytes());
    response.extend_from_slice(&ttl.to_be_bytes());
    response.extend_from_slice(&(data.len() as u16).to_be_bytes());
    response.extend_from_slice(data);
}

pub(super) fn encoded_name(name: &str) -> Vec<u8> {
    let mut output = Vec::new();
    encode_name(name, &mut output);
    output
}

pub(super) fn compression_pointer(offset: usize) -> [u8; 2] {
    assert!(offset <= 0x3fff, "DNS compression pointer offset");
    [0xc0 | ((offset >> 8) as u8), offset as u8]
}

fn question_end(message: &[u8]) -> usize {
    let mut offset = 12;
    while message[offset] != 0 {
        offset += usize::from(message[offset]) + 1;
    }
    offset + 5
}

fn encode_name(name: &str, output: &mut Vec<u8>) {
    for label in name.split('.') {
        output.push(label.len() as u8);
        output.extend_from_slice(label.as_bytes());
    }
    output.push(0);
}
