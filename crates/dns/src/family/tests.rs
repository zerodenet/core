use super::*;

#[test]
fn mapped_addresses_are_ipv4_before_family_filtering() {
    let mapped = IpAddress::V6("::ffff:192.0.2.7".parse::<std::net::Ipv6Addr>().unwrap().octets());
    let ipv4 = IpAddress::V4([192, 0, 2, 7]);
    for family in [AddressFamily::Auto, AddressFamily::OnlyIpv4] {
        assert_eq!(filter_addresses("mapped.test", vec![mapped], family).unwrap(), vec![ipv4]);
    }
    assert_eq!(filter_addresses("mapped.test", vec![mapped], AddressFamily::OnlyIpv6).unwrap_err().kind(), io::ErrorKind::NotFound);
}
