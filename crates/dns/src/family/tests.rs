use super::*;

#[test]
fn mapped_addresses_are_ipv4_before_family_filtering() {
    let mapped = IpAddress::V6(
        "::ffff:192.0.2.7"
            .parse::<std::net::Ipv6Addr>()
            .unwrap()
            .octets(),
    );
    let ipv4 = IpAddress::V4([192, 0, 2, 7]);
    assert_eq!(
        filter_addresses("mapped.test", vec![mapped], AddressFamily::OnlyIpv4).unwrap(),
        vec![ipv4]
    );
    assert_eq!(
        filter_addresses("mapped.test", vec![mapped], AddressFamily::Auto).unwrap(),
        vec![mapped],
        "automatic resolution must preserve the original DNS RR representation"
    );
    assert_eq!(
        filter_addresses("mapped.test", vec![mapped], AddressFamily::OnlyIpv6)
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
}
