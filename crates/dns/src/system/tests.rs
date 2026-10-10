use super::*;

#[tokio::test]
async fn mapped_system_candidates_are_normalized_only_for_strict_business_families() {
    let resolver = TokioSystemResolver;
    let domain = "::ffff:127.0.0.1";
    let mapped = IpAddress::V6(domain.parse::<std::net::Ipv6Addr>().unwrap().octets());
    assert_eq!(
        resolver
            .resolve_type(domain, crate::message::TYPE_AAAA, AddressFamily::Auto)
            .await
            .unwrap(),
        vec![mapped]
    );
    assert_eq!(
        resolver
            .resolve_type(domain, crate::message::TYPE_A, AddressFamily::OnlyIpv4)
            .await
            .unwrap(),
        vec![IpAddress::V4([127, 0, 0, 1])]
    );
    assert_eq!(
        resolver
            .resolve_type(domain, crate::message::TYPE_AAAA, AddressFamily::OnlyIpv6)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
}
