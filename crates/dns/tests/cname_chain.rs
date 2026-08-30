#![cfg(feature = "udp")]

use std::io;

use zero_config::{DnsCacheConfig, DnsReverseMappingConfig};
use zero_dns::RealIpReverseLookup;
use zero_traits::IpAddress;

#[path = "cname_chain/support.rs"]
mod support;

use support::{
    append_record, compression_pointer, config, encoded_name, resolve_once, response_header,
    TYPE_A, TYPE_AAAA, TYPE_CNAME,
};

#[tokio::test]
async fn direct_a_and_aaaa_answers_remain_valid() {
    let ipv4 = resolve_once("direct.example", TYPE_A, |query| {
        let mut response = response_header(query, 1);
        append_record(
            &mut response,
            "direct.example",
            TYPE_A,
            60,
            &[192, 0, 2, 10],
        );
        response
    })
    .await
    .expect("resolve direct A answer");
    assert_eq!(ipv4, vec![IpAddress::V4([192, 0, 2, 10])]);

    let expected_v6 = [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10];
    let ipv6 = resolve_once("direct.example", TYPE_AAAA, move |query| {
        let mut response = response_header(query, 1);
        append_record(&mut response, "direct.example", TYPE_AAAA, 60, &expected_v6);
        response
    })
    .await
    .expect("resolve direct AAAA answer");
    assert_eq!(ipv6, vec![IpAddress::V6(expected_v6)]);
}

#[tokio::test]
async fn unrelated_answer_owner_is_not_a_resolution_candidate() {
    let result = resolve_once("victim.example", TYPE_A, |query| {
        let mut response = response_header(query, 1);
        append_record(
            &mut response,
            "unrelated.example",
            TYPE_A,
            60,
            &[203, 0, 113, 99],
        );
        response
    })
    .await;

    let error = result.expect_err("unrelated answer owner must not resolve the query");
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
}

#[tokio::test]
async fn unrelated_address_is_not_cached_or_reverse_mapped() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind instrumented DNS server");
    let port = socket.local_addr().expect("DNS endpoint").port();
    let server = tokio::spawn(async move {
        for (owner, address) in [
            ("unrelated.example", [203, 0, 113, 99]),
            ("victim.example", [192, 0, 2, 40]),
        ] {
            let mut request = [0_u8; 4096];
            let (size, peer) = socket
                .recv_from(&mut request)
                .await
                .expect("receive DNS query");
            let mut response = response_header(&request[..size], 1);
            append_record(&mut response, owner, TYPE_A, 60, &address);
            socket
                .send_to(&response, peer)
                .await
                .expect("send DNS response");
        }
    });

    let mut dns_config = config(port);
    dns_config.cache = Some(DnsCacheConfig {
        max_entries: 16,
        max_ttl_seconds: Some(60),
    });
    dns_config.reverse_mapping = Some(DnsReverseMappingConfig {
        max_entries: 16,
        max_domains_per_address: 4,
        max_ttl_seconds: 60,
    });
    let egress = zero_platform_tokio::EgressInterfaceControl::default();
    let dns = zero_dns::DnsSystem::build_with_egress(Some(&dns_config), egress.clone())
        .expect("build DNS system");

    let first = dns.resolve_real_type("victim.example", TYPE_A).await;
    assert_eq!(
        first.expect_err("unrelated answer must be NODATA").kind(),
        io::ErrorKind::NotFound
    );
    assert_eq!(
        dns.lookup_real_ip(&IpAddress::V4([203, 0, 113, 99])).await,
        RealIpReverseLookup::Missing
    );

    // A topology generation change deliberately invalidates the short-lived
    // query-coordinator failure cache, so this second lookup proves that no
    // address-cache entry was written for the unrelated answer.
    egress.replace_tunnel_addresses(["10.66.0.1".parse().expect("TUN address")]);
    assert_eq!(
        dns.resolve_real_type("victim.example", TYPE_A)
            .await
            .expect("second query reaches upstream"),
        vec![IpAddress::V4([192, 0, 2, 40])]
    );
    assert_eq!(
        dns.lookup_real_ip(&IpAddress::V4([192, 0, 2, 40])).await,
        RealIpReverseLookup::Resolved("victim.example".to_owned())
    );
    server.await.expect("instrumented DNS server task");
}

#[tokio::test]
async fn resolves_one_hop_cname_with_compressed_target() {
    let addresses = resolve_once("alias.example", TYPE_A, |query| {
        let mut response = response_header(query, 2);
        let terminal_owner_offset = response.len();
        append_record(
            &mut response,
            "canonical.example",
            TYPE_A,
            60,
            &[192, 0, 2, 20],
        );
        let target = compression_pointer(terminal_owner_offset);
        append_record(&mut response, "alias.example", TYPE_CNAME, 60, &target);
        response
    })
    .await
    .expect("resolve compressed CNAME target");

    assert_eq!(addresses, vec![IpAddress::V4([192, 0, 2, 20])]);
}

#[tokio::test]
async fn resolves_unordered_multi_hop_chain_and_ignores_unrelated_address() {
    let addresses = resolve_once("root.example", TYPE_A, |query| {
        let mut response = response_header(query, 4);
        append_record(
            &mut response,
            "unrelated.example",
            TYPE_A,
            60,
            &[203, 0, 113, 77],
        );
        append_record(
            &mut response,
            "terminal.example",
            TYPE_A,
            60,
            &[192, 0, 2, 30],
        );
        append_record(
            &mut response,
            "middle.example",
            TYPE_CNAME,
            60,
            &encoded_name("terminal.example"),
        );
        append_record(
            &mut response,
            "root.example",
            TYPE_CNAME,
            60,
            &encoded_name("middle.example"),
        );
        response
    })
    .await
    .expect("resolve unordered multi-hop CNAME chain");

    assert_eq!(addresses, vec![IpAddress::V4([192, 0, 2, 30])]);
}

#[tokio::test]
async fn invalid_trusted_cname_chains_fail_closed() {
    let loop_error = resolve_once("loop.example", TYPE_A, |query| {
        let mut response = response_header(query, 2);
        append_record(
            &mut response,
            "loop.example",
            TYPE_CNAME,
            60,
            &encoded_name("middle.example"),
        );
        append_record(
            &mut response,
            "middle.example",
            TYPE_CNAME,
            60,
            &encoded_name("loop.example"),
        );
        response
    })
    .await
    .expect_err("CNAME loop must fail closed");
    assert_eq!(loop_error.kind(), io::ErrorKind::TimedOut);

    let conflict_error = resolve_once("conflict.example", TYPE_A, |query| {
        let mut response = response_header(query, 2);
        append_record(
            &mut response,
            "conflict.example",
            TYPE_CNAME,
            60,
            &encoded_name("one.example"),
        );
        append_record(
            &mut response,
            "conflict.example",
            TYPE_CNAME,
            60,
            &encoded_name("two.example"),
        );
        response
    })
    .await
    .expect_err("conflicting CNAME targets must fail closed");
    assert_eq!(conflict_error.kind(), io::ErrorKind::TimedOut);

    let malformed_error = resolve_once("malformed.example", TYPE_A, |query| {
        let mut response = response_header(query, 1);
        append_record(&mut response, "malformed.example", TYPE_CNAME, 60, &[0xc0]);
        response
    })
    .await
    .expect_err("malformed CNAME RDATA must fail closed");
    assert_eq!(malformed_error.kind(), io::ErrorKind::TimedOut);
}
