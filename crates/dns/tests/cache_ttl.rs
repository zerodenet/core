#![cfg(feature = "udp")]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use zero_config::{DnsAnswerConfig, DnsCacheConfig, DnsConfig, DnsServerConfig};

const CACHE_TTL_CAP: u64 = 5;
const OPT_METADATA: u32 = 0x0100_8000;

#[tokio::test]
async fn wire_cache_caps_and_ages_resource_record_ttls_without_touching_opt() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind instrumented DNS server");
    let port = socket.local_addr().expect("DNS server address").port();
    let query_count = Arc::new(AtomicUsize::new(0));
    let server_query_count = Arc::clone(&query_count);
    let server = tokio::spawn(async move {
        let mut request = [0_u8; 4096];
        loop {
            let (size, peer) = socket
                .recv_from(&mut request)
                .await
                .expect("receive DNS query");
            server_query_count.fetch_add(1, Ordering::SeqCst);
            let response = response_with_mixed_ttls(&request[..size]);
            socket
                .send_to(&response, peer)
                .await
                .expect("send DNS response");
        }
    });
    let dns = zero_dns::DnsSystem::build(Some(&config(port))).expect("build DNS runtime");

    let first_query = query(0x4217);
    let first = dns
        .answer_udp_query(&first_query)
        .await
        .expect("answer first intercepted query");
    let first_records = records(&first);

    assert_eq!(&first[..2], &0x4217_u16.to_be_bytes());
    assert_eq!(
        first_records,
        vec![(1, 5), (6, 5), (1, 5), (16, 4), (41, OPT_METADATA)]
    );

    tokio::time::sleep(Duration::from_millis(1_100)).await;

    let second_query = query(0x9911);
    let second = dns
        .answer_tcp_query(&second_query)
        .await
        .expect("answer cached intercepted query");
    let second_records = records(&second);

    assert_eq!(&second[..2], &0x9911_u16.to_be_bytes());
    assert_eq!(
        second_records
            .iter()
            .map(|record| record.0)
            .collect::<Vec<_>>(),
        vec![1, 6, 1, 16, 41]
    );
    for (first, second) in first_records.iter().zip(&second_records) {
        if first.0 == 41 {
            assert_eq!(second.1, OPT_METADATA, "EDNS OPT metadata was rewritten");
        } else {
            assert!(
                second.1 < first.1,
                "cached TTL did not age: first={first:?}, second={second:?}"
            );
            assert!(
                second.1 <= 3,
                "cached TTL exceeded the remaining whole-response lifetime: {second:?}"
            );
        }
    }

    let third_query = query(0xa255);
    let third = dns
        .answer_udp_query(&third_query)
        .await
        .expect("answer repeated cache hit");
    assert_eq!(&third[..2], &0xa255_u16.to_be_bytes());
    assert_eq!(
        records(&third),
        second_records,
        "cache-hit aging compounded instead of using total elapsed time"
    );

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        query_count.load(Ordering::SeqCst),
        1,
        "cache hit unexpectedly queried the upstream DNS server"
    );
    server.abort();
}

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
        cache: Some(DnsCacheConfig {
            max_entries: 8,
            max_ttl_seconds: Some(CACHE_TTL_CAP),
        }),
        reverse_mapping: None,
        answer: DnsAnswerConfig::Real,
        policy: Default::default(),
    }
}

fn query(id: u16) -> Vec<u8> {
    let mut query = Vec::from(id.to_be_bytes());
    query.extend_from_slice(&[
        0x01, 0x00, // standard recursive query
        0x00, 0x01, // one question
        0x00, 0x00, // no answers
        0x00, 0x00, // no authority records
        0x00, 0x01, // one OPT pseudo-record
    ]);
    for label in ["cache", "example"] {
        query.push(label.len() as u8);
        query.extend_from_slice(label.as_bytes());
    }
    query.push(0);
    query.extend_from_slice(&1_u16.to_be_bytes());
    query.extend_from_slice(&1_u16.to_be_bytes());
    append_record_with_owner(&mut query, &[0], 41, 1232, 0, &[]);
    query
}

fn response_with_mixed_ttls(query: &[u8]) -> Vec<u8> {
    zero_dns::udp::parse_dns_question(query).expect("parse test query");
    let mut response = Vec::new();
    response.extend_from_slice(&query[..2]);
    response.extend_from_slice(&[
        0x81, 0x80, // standard successful recursive response
        0x00, 0x01, // one question
        0x00, 0x01, // one answer
        0x00, 0x01, // one authority record
        0x00, 0x03, // three additional records
    ]);
    response.extend_from_slice(&query[12..question_end(query)]);
    append_record(&mut response, 1, 1, 120, &[192, 0, 2, 1]);
    append_record(
        &mut response,
        6,
        1,
        90,
        &[
            0, 0, // root MNAME and RNAME
            0, 0, 0, 1, // serial
            0, 0, 0, 60, // refresh
            0, 0, 0, 60, // retry
            0, 0, 0, 60, // expire
            0, 0, 0, 60, // minimum
        ],
    );
    append_record(&mut response, 1, 1, 75, &[192, 0, 2, 2]);
    append_record(&mut response, 16, 1, 4, &[1, b'x']);
    append_record_with_owner(&mut response, &[0], 41, 1232, OPT_METADATA, &[]);
    response
}

fn append_record(response: &mut Vec<u8>, record_type: u16, class: u16, ttl: u32, data: &[u8]) {
    append_record_with_owner(response, &[0xc0, 0x0c], record_type, class, ttl, data);
}

fn append_record_with_owner(
    response: &mut Vec<u8>,
    owner: &[u8],
    record_type: u16,
    class: u16,
    ttl: u32,
    data: &[u8],
) {
    response.extend_from_slice(owner);
    response.extend_from_slice(&record_type.to_be_bytes());
    response.extend_from_slice(&class.to_be_bytes());
    response.extend_from_slice(&ttl.to_be_bytes());
    response.extend_from_slice(&(data.len() as u16).to_be_bytes());
    response.extend_from_slice(data);
}

fn records(response: &[u8]) -> Vec<(u16, u32)> {
    let count = usize::from(u16::from_be_bytes([response[6], response[7]]))
        + usize::from(u16::from_be_bytes([response[8], response[9]]))
        + usize::from(u16::from_be_bytes([response[10], response[11]]));
    let mut offset = question_end(response);
    let mut records = Vec::new();
    for _ in 0..count {
        let name_end = skip_name(response, offset);
        let record_type = u16::from_be_bytes([response[name_end], response[name_end + 1]]);
        let ttl = u32::from_be_bytes([
            response[name_end + 4],
            response[name_end + 5],
            response[name_end + 6],
            response[name_end + 7],
        ]);
        let length = usize::from(u16::from_be_bytes([
            response[name_end + 8],
            response[name_end + 9],
        ]));
        records.push((record_type, ttl));
        offset = name_end + 10 + length;
    }
    assert_eq!(offset, response.len());
    records
}

fn question_end(message: &[u8]) -> usize {
    skip_name(message, 12) + 4
}

fn skip_name(message: &[u8], mut offset: usize) -> usize {
    loop {
        let length = message[offset];
        if length & 0xc0 == 0xc0 {
            return offset + 2;
        }
        offset += 1;
        if length == 0 {
            return offset;
        }
        offset += usize::from(length);
    }
}
