use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use zero_core::{Address, Network, Session};
use zero_dns::DnsSystem;
use zero_traits::IpAddress;

use super::socket_addr_from_ip;

pub(in crate::transport) const MAX_RECOVERED_DIRECT_CANDIDATES: usize = 8;

#[derive(Debug, Clone)]
pub(super) struct RecoveredDirectCandidateRefresh {
    pub(super) domain: String,
    pub(super) host_source: &'static str,
}

/// Identify a transparent TCP target that may use trusted real-DNS answers
/// only after its authoritative captured IPv4 endpoint fails to connect.
pub(super) fn recovered_ipv4_candidate_refresh(
    session: &Session,
    candidates: &[SocketAddr],
) -> Option<RecoveredDirectCandidateRefresh> {
    if session.network != Network::Tcp {
        return None;
    }
    let (Address::Domain(domain), Some(host_source), Some(Address::Ipv4(original_address))) = (
        &session.target,
        session.target_host_source,
        session.direct_target.as_ref(),
    ) else {
        return None;
    };
    let original = SocketAddr::new(IpAddr::V4(Ipv4Addr::from(*original_address)), session.port);
    if candidates.first() != Some(&original) {
        return None;
    }

    Some(RecoveredDirectCandidateRefresh {
        domain: domain.clone(),
        host_source: host_source.as_str(),
    })
}

/// Append current real-DNS answers after the captured candidate has failed.
/// DNS failure is returned to the caller so it can retain the original socket
/// failure and its platform error as the authoritative result.
pub(super) async fn refresh_recovered_ipv4_candidates(
    refresh: &RecoveredDirectCandidateRefresh,
    resolver: &DnsSystem,
    port: u16,
    mut candidates: Vec<SocketAddr>,
) -> std::io::Result<Vec<SocketAddr>> {
    let resolved = resolver.resolve_direct(&refresh.domain).await?;
    append_unique_resolved_candidates(&mut candidates, resolved, port);
    Ok(candidates)
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
