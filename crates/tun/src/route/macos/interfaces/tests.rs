use super::{classify, usable_address};
use crate::route::EgressUnavailableReason;

#[test]
fn interface_return_requires_both_link_and_family_address() {
    assert_eq!(
        classify(false, true),
        Some(EgressUnavailableReason::InterfaceDown)
    );
    assert_eq!(
        classify(true, false),
        Some(EgressUnavailableReason::NoUsableAddress)
    );
    assert_eq!(classify(true, true), None);
}

#[test]
fn link_local_or_placeholder_address_is_not_an_internet_egress() {
    for address in [
        "0.0.0.0",
        "127.0.0.1",
        "169.254.1.2",
        "224.0.0.1",
        "255.255.255.255",
        "::",
        "::1",
        "fe80::1",
        "ff02::1",
    ] {
        assert!(!usable_address(address.parse().unwrap()), "{address}");
    }
    for address in ["192.168.0.2", "10.0.0.2", "2001:db8::2", "fd00::2"] {
        assert!(usable_address(address.parse().unwrap()), "{address}");
    }
}
