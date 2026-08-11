use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

#[test]
fn split_defaults_cover_each_address_family_without_replacing_default() {
    assert_eq!(
        zero_tun::split_default_route_prefixes(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
        ["0.0.0.0/1", "128.0.0.0/1"]
    );
    assert_eq!(
        zero_tun::split_default_route_prefixes(IpAddr::V6(Ipv6Addr::LOCALHOST)),
        ["::/1", "8000::/1"]
    );
}
