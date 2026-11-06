//! ICMP Time Exceeded for packets stopped by an L3 forwarding hop.

use std::net::IpAddr;

/// Local path-MTU error with an explicitly supplied router address. The
/// original quote is unchanged; only the outer source/checksums are rebuilt.
pub fn build_icmp_mtu_error_response(
    original: &[u8],
    router_source: IpAddr,
    mtu: usize,
) -> Option<Vec<u8>> {
    super::IcmpErrorSpec::for_mtu_error(original, router_source, mtu).map(|spec| spec.allocate())
}
pub fn build_icmp_time_exceeded_response(
    original: &[u8],
    router_source: IpAddr,
    mtu: usize,
) -> Option<Vec<u8>> {
    super::IcmpErrorSpec::for_time_exceeded(original, router_source, mtu)
        .map(|spec| spec.allocate())
}
