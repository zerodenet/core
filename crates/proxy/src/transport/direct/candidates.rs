use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use zero_core::{Address, Network, Session};
use zero_dns::DnsSystem;
use zero_traits::IpAddress;

use super::socket_addr_from_ip;

pub(in crate::transport) const MAX_RECOVERED_DIRECT_CANDIDATES: usize = 8;

/// Enrich a transparent IPv4 direct target with the current real-DNS answers
/// for its recovered domain. The captured endpoint remains first, and DNS
/// failure is deliberately non-fatal because that endpoint is still a valid
/// literal target.
pub(super) async fn append_recovered_ipv4_candidates(
    session: &Session,
    resolver: &DnsSystem,
    mut candidates: Vec<SocketAddr>,
) -> Vec<SocketAddr> {
    if session.network != Network::Tcp {
        return candidates;
    }
    let (Address::Domain(domain), Some(host_source), Some(Address::Ipv4(original_address))) = (
        &session.target,
        session.target_host_source,
        session.direct_target.as_ref(),
    ) else {
        return candidates;
    };
    let original = SocketAddr::new(IpAddr::V4(Ipv4Addr::from(*original_address)), session.port);
    if candidates.first() != Some(&original) {
        return candidates;
    }

    match resolver.resolve_direct(domain).await {
        Ok(resolved) => {
            append_unique_resolved_candidates(&mut candidates, resolved, session.port);
            tracing::debug!(
                original_target = %original,
                domain,
                host_source = host_source.as_str(),
                candidate_count = candidates.len(),
                "enriched transparent direct target with trusted DNS candidates"
            );
        }
        Err(error) => {
            tracing::debug!(
                original_target = %original,
                domain,
                host_source = host_source.as_str(),
                error = %error,
                "trusted-domain candidate refresh failed; retaining original direct target"
            );
        }
    }
    candidates
}

pub(in crate::transport) fn append_unique_resolved_candidates(
    candidates: &mut Vec<SocketAddr>,
    resolved: Vec<IpAddress>,
    port: u16,
) {
    for address in resolved {
        if candidates.len() >= MAX_RECOVERED_DIRECT_CANDIDATES {
            break;
        }
        let candidate = socket_addr_from_ip(address, port);
        if !candidates.contains(&candidate) {
            candidates.push(candidate);
        }
    }
}

pub(super) fn literal_direct_target(session: &Session) -> Option<SocketAddr> {
    match session.direct_target.as_ref()? {
        Address::Ipv4(address) => Some(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::from(*address)),
            session.port,
        )),
        Address::Ipv6(address) => Some(SocketAddr::new(
            IpAddr::V6(Ipv6Addr::from(*address)),
            session.port,
        )),
        Address::Domain(_) => None,
    }
}
