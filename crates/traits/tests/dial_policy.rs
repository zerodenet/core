use core::net::{IpAddr, SocketAddr};
use zero_traits::{canonicalize_ip, AddressFamily, DialPolicy, DialPolicyError};
#[test]
fn source_narrows_auto_and_mapped_addresses_are_ipv4() {
    let policy = DialPolicy { source_ip: Some("::ffff:127.0.0.1".parse().unwrap()), ..Default::default() };
    assert_eq!(policy.effective_family(), Ok(AddressFamily::OnlyIpv4));
    assert!(policy.allows_ip("::ffff:192.0.2.1".parse().unwrap()));
    assert!(!policy.allows_ip("::1".parse().unwrap()));
    assert_eq!(canonicalize_ip("::ffff:127.0.0.1".parse().unwrap()), "127.0.0.1".parse::<IpAddr>().unwrap());
}
#[test]
fn conflicting_source_family_never_allows_a_destination() {
    let policy = DialPolicy { address_family: AddressFamily::OnlyIpv6,
        source_ip: Some("::ffff:127.0.0.1".parse().unwrap()), ..Default::default() };
    assert_eq!(policy.validate(), Err(DialPolicyError::SourceFamilyConflict));
    assert!(!policy.allows_ip("127.0.0.1".parse().unwrap()));
    assert!(!policy.allows_ip("::1".parse().unwrap()));
}
#[test]
fn filtering_retains_order_and_normalizes_mapped_candidates() {
    let policy = DialPolicy { address_family: AddressFamily::OnlyIpv4, ..Default::default() };
    let candidates = ["[::1]:80", "192.0.2.2:80", "[::ffff:192.0.2.1]:80"].map(|address| address.parse::<SocketAddr>().unwrap());
    assert_eq!(policy.filter_candidates(candidates).unwrap(), ["192.0.2.2:80".parse::<SocketAddr>().unwrap(), "192.0.2.1:80".parse().unwrap()]);
    let scoped: SocketAddr = "[fe80::1%7]:80".parse().unwrap();
    assert_eq!(DialPolicy::default().normalize_peer(scoped).unwrap(), scoped);
}
#[test]
fn only_ipv6_rejects_mapped_v4_without_fallback() {
    let policy = DialPolicy { address_family: AddressFamily::OnlyIpv6, ..Default::default() };
    assert_eq!(policy.normalize_peer("[::ffff:127.0.0.1]:80".parse().unwrap()), Err(DialPolicyError::DestinationFamilyMismatch));
    assert!(policy.filter_candidates(["127.0.0.1:80".parse().unwrap()]).unwrap().is_empty());
}
#[test]
fn structural_validation_rejects_unusable_sources_and_interface_names() {
    for source in ["0.0.0.0", "::", "224.0.0.1", "ff02::1", "255.255.255.255", "::ffff:0.0.0.0", "::ffff:224.0.0.1"] {
        let policy = DialPolicy { source_ip: Some(source.parse().unwrap()), ..Default::default() };
        assert_eq!(policy.validate(), Err(DialPolicyError::InvalidSourceIp), "{source}");
    }
    for interface in ["", " ", " eth0", "eth0 ", "eth\0", "eth\t0", "eth/0"] {
        let policy = DialPolicy { interface: Some(interface.into()), ..Default::default() };
        assert_eq!(policy.validate(), Err(DialPolicyError::InvalidInterface), "{interface:?}");
    }
    assert!(DialPolicy { interface: Some("Ethernet 2".into()), ..Default::default() }.validate().is_ok());
}
