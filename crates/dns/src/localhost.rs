//! RFC 6761 localhost names never depend on upstream DNS or Fake-IP state.

use zero_config::DnsAddressFamilyPolicy;
use zero_traits::IpAddress;

const IPV4: IpAddress = IpAddress::V4([127, 0, 0, 1]);
const IPV6: IpAddress = IpAddress::V6([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);

/// The caller normalizes and validates the DNS name before matching labels.
pub(crate) fn is_localhost(domain: &str) -> bool {
    domain == "localhost" || domain.ends_with(".localhost")
}

pub(crate) fn addresses(policy: DnsAddressFamilyPolicy) -> Vec<IpAddress> {
    match policy {
        DnsAddressFamilyPolicy::Ipv4Only => vec![IPV4],
        DnsAddressFamilyPolicy::Ipv6Only => vec![IPV6],
        DnsAddressFamilyPolicy::PreferIpv4 => vec![IPV4, IPV6],
        DnsAddressFamilyPolicy::PreferIpv6 => vec![IPV6, IPV4],
    }
}

pub(crate) fn address_records(query_type: u16) -> Vec<IpAddress> {
    match query_type {
        crate::message::TYPE_A => vec![IPV4],
        crate::message::TYPE_AAAA => vec![IPV6],
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests;
