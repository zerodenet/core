use zero_core::{Address, FakeIpReverseStatus, Session, TargetHostSource};
use zero_dns::{DnsSystem, RealIpReverseLookup};
use zero_engine::EngineError;
use zero_traits::IpAddress;

mod failure;
pub(crate) use failure::finish_target_recovery_failure;

/// Recover a logical DNS target while preserving transparent direct semantics.
pub(crate) async fn resolve_dns_target(
    resolver: &DnsSystem,
    session: &mut Session,
) -> Result<(), EngineError> {
    let current_ip = address_ip(&session.target);
    let original_ip = session.original_target.as_ref().and_then(address_ip);
    let synthetic = current_ip
        .filter(|(_, standard_ip)| resolver.fake_ip_contains(*standard_ip))
        .or_else(|| original_ip.filter(|(_, standard_ip)| resolver.fake_ip_contains(*standard_ip)));

    if let Some((ip, standard_ip)) = synthetic {
        let synthetic_target = address_from_ip(ip);
        if session.original_target.is_none() {
            session.original_target = Some(synthetic_target.clone());
        }
        if let Some(domain) = resolver.lookup_fake_ip(&ip).await {
            session.target = Address::Domain(domain);
            session.direct_target = None;
            session.target_host_source = Some(TargetHostSource::FakeIp);
            session.fake_ip_reverse_status = Some(FakeIpReverseStatus::Resolved);
        } else {
            session.target = synthetic_target;
            session.direct_target = None;
            session.target_host_source = None;
            session.fake_ip_reverse_status = Some(FakeIpReverseStatus::Missing);
            return Err(EngineError::FakeIpReverseMissing {
                address: standard_ip.to_string(),
            });
        }
        return Ok(());
    }

    let Some((ip, _)) = current_ip else {
        return Ok(());
    };
    if !session.transparent_target {
        return Ok(());
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
    Ok(())
}

fn address_ip(address: &Address) -> Option<(IpAddress, std::net::IpAddr)> {
    match address {
        Address::Ipv4(octets) => Some((
            IpAddress::V4(*octets),
            std::net::IpAddr::V4((*octets).into()),
        )),
        Address::Ipv6(octets) => Some((
            IpAddress::V6(*octets),
            std::net::IpAddr::V6((*octets).into()),
        )),
        Address::Domain(_) => None,
    }
}

fn address_from_ip(address: IpAddress) -> Address {
    match address {
        IpAddress::V4(octets) => Address::Ipv4(octets),
        IpAddress::V6(octets) => Address::Ipv6(octets),
    }
}

#[cfg(test)]
mod tests;
