//! Resolve inner packet destinations independently of the host's direct egress.

use std::net::{IpAddr, SocketAddr};

use zero_core::{Address, Session};
use zero_dns::DnsSystem;
use zero_engine::EngineError;
use zero_traits::IpAddress;

pub(crate) async fn resolve_packet_targets(
    session: &Session,
    resolver: &DnsSystem,
) -> Result<Vec<SocketAddr>, EngineError> {
    if session.port == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "packet target port is required",
        )
        .into());
    }
    let addresses = match &session.target {
        Address::Ipv4(octets) => vec![IpAddress::V4(*octets)],
        Address::Ipv6(octets) => vec![IpAddress::V6(*octets)],
        // This is the business destination inside the tunnel. Direct and
        // Node roles describe host egress and outer carrier addresses instead.
        Address::Domain(domain) => resolver.resolve_real(domain).await?,
    };
    let candidates: Vec<_> = addresses
        .into_iter()
        .map(|address| {
            let ip = match address {
                IpAddress::V4(octets) => IpAddr::V4(octets.into()),
                IpAddress::V6(octets) => IpAddr::V6(octets.into()),
            };
            SocketAddr::new(ip, session.port)
        })
        .collect();
    if candidates.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "packet target resolved to no IP addresses",
        )
        .into());
    }
    Ok(candidates)
}
