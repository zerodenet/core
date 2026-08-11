//! Tests for `prefix_to_mask`.
//!
//! Migrated from the inline `#[cfg(test)] mod tests` in
//! `crates/tun/src/lib.rs`.

use std::net::IpAddr;

use zero_tun::{mask_to_prefix, prefix_to_mask};

#[test]
fn test_prefix_to_mask_v4() {
    let m = prefix_to_mask(24, false);
    assert_eq!(m, IpAddr::V4(std::net::Ipv4Addr::new(255, 255, 255, 0)));
}

#[test]
fn test_prefix_to_mask_v6() {
    let m = prefix_to_mask(64, true);
    let expected = std::net::Ipv6Addr::new(0xffff, 0xffff, 0xffff, 0xffff, 0, 0, 0, 0);
    assert_eq!(m, IpAddr::V6(expected));
}

#[test]
fn mask_to_prefix_round_trips_both_families() {
    assert_eq!(mask_to_prefix(prefix_to_mask(24, false)).unwrap(), 24);
    assert_eq!(mask_to_prefix(prefix_to_mask(64, true)).unwrap(), 64);
    assert_eq!(mask_to_prefix(prefix_to_mask(0, false)).unwrap(), 0);
    assert_eq!(mask_to_prefix(prefix_to_mask(128, true)).unwrap(), 128);
}

#[test]
fn mask_to_prefix_rejects_non_contiguous_masks() {
    let mask = IpAddr::V4(std::net::Ipv4Addr::new(255, 0, 255, 0));
    assert!(mask_to_prefix(mask).is_err());
}
