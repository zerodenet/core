#![cfg(any(not(feature = "udp"), not(feature = "doh")))]

async fn assert_transport_unavailable(endpoint: &str, transport: &str) {
    let dns = zero_dns::DnsSystem::build(None).unwrap();
    let error = dns
        .query_ech_config("secret.example", endpoint)
        .await
        .expect_err("disabled ECH transport must fail explicitly");
    assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
    assert!(error.to_string().contains(transport));
    assert!(error.to_string().contains("not compiled"));
}

#[cfg(not(feature = "udp"))]
#[tokio::test]
async fn rejects_ech_udp_query_without_udp_support() {
    assert_transport_unavailable("udp://127.0.0.1:53", "UDP").await;
}

#[cfg(not(feature = "doh"))]
#[tokio::test]
async fn rejects_ech_https_query_without_doh_support() {
    for endpoint in ["https://127.0.0.1/dns-query", "h2c://127.0.0.1/dns-query"] {
        assert_transport_unavailable(endpoint, "DNS-over-HTTPS").await;
    }
}
