use zero_core::{Address, FakeIpReverseStatus, Session, TargetHostSource};
use zero_dns::{DnsSystem, RealIpReverseLookup};
use zero_traits::IpAddress;

/// Recover a logical DNS target while preserving transparent direct semantics.
pub(crate) async fn resolve_dns_target(resolver: &DnsSystem, session: &mut Session) {
    let (ip, standard_ip) = match &session.target {
        Address::Ipv4(octets) => (
            IpAddress::V4(*octets),
            std::net::IpAddr::V4((*octets).into()),
        ),
        Address::Ipv6(octets) => (
            IpAddress::V6(*octets),
            std::net::IpAddr::V6((*octets).into()),
        ),
        _ => return,
    };

    if resolver.fake_ip_contains(standard_ip) {
        session.original_target = Some(session.target.clone());
        if let Some(domain) = resolver.lookup_fake_ip(&ip).await {
            session.target = Address::Domain(domain);
            session.target_host_source = Some(TargetHostSource::FakeIp);
            session.fake_ip_reverse_status = Some(FakeIpReverseStatus::Resolved);
        } else {
            session.fake_ip_reverse_status = Some(FakeIpReverseStatus::Missing);
        }
        return;
    }

    if !session.transparent_target {
        return;
    }
    match resolver.lookup_real_ip(&ip).await {
        RealIpReverseLookup::Resolved(domain) => {
            let original = session.target.clone();
            session.original_target = Some(original.clone());
            session.direct_target = Some(original);
            session.target = Address::Domain(domain);
            session.target_host_source = Some(TargetHostSource::DnsReverse);
        }
        RealIpReverseLookup::Ambiguous => {
            tracing::trace!(target = ?session.target, "real-IP reverse lookup is ambiguous");
        }
        RealIpReverseLookup::Missing => {}
    }
}

#[cfg(test)]
mod tests;
