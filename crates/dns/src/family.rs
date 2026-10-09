//! Per-business-resolution constraints. These never configure DNS transport sockets.

use std::io;
use std::net::IpAddr;
use std::sync::atomic::Ordering;

use zero_config::DnsAddressFamilyPolicy;
use zero_traits::{canonicalize_ip, AddressFamily, IpAddress};

use crate::coordinator::QueryScope;
use crate::{message, resolve_snapshot, DnsQueryRole, DnsSystem, ResolveSnapshot};

impl DnsSystem {
    /// Resolve a real Direct target with an optional per-call family constraint.
    ///
    /// `Auto` preserves the configured DNS address-family policy. A strict
    /// family overrides that default for this lookup only, retaining Direct's
    /// selected resolver and fallback chain. Built-in DNS sends only A or AAAA;
    /// a System backend uses the OS lookup and filters its returned candidates.
    /// DNS upstream sockets, node lookups, and intercepted queries are unchanged.
    pub async fn resolve_direct_with_family(
        &self,
        domain: &str,
        family: AddressFamily,
    ) -> io::Result<Vec<IpAddress>> {
        if family == AddressFamily::Auto {
            return self.resolve_direct(domain).await;
        }
        let domain = message::normalize_domain(domain)?;
        let (snapshot, config_generation) = self.snapshot_with_generation();
        match snapshot {
            Some(mut snapshot) => {
                snapshot.family = family;
                snapshot.policy.address_family = match family {
                    AddressFamily::OnlyIpv4 => DnsAddressFamilyPolicy::Ipv4Only,
                    AddressFamily::OnlyIpv6 => DnsAddressFamilyPolicy::Ipv6Only,
                    AddressFamily::Auto => unreachable!("automatic family returned above"),
                };
                resolve_snapshot(&domain, DnsQueryRole::Direct, snapshot).await
            }
            None => {
                let query_type = match family {
                    AddressFamily::OnlyIpv4 => message::TYPE_A,
                    AddressFamily::OnlyIpv6 => message::TYPE_AAAA,
                    AddressFamily::Auto => unreachable!("automatic family returned above"),
                };
                self.resolve_system_type_with_family_coordinated(
                    &domain,
                    query_type,
                    DnsQueryRole::Direct,
                    family,
                    config_generation,
                )
                .await
            }
        }
    }
}

impl ResolveSnapshot {
    pub(crate) fn query_scope(&self, role: DnsQueryRole) -> QueryScope {
        QueryScope {
            role,
            family: self.family,
            config_generation: self.config_generation,
            egress_generation: self.egress_generation,
        }
    }

    pub(crate) fn is_current(&self) -> bool {
        self.egress_interface.generation() == self.egress_generation
            && self.current_config_generation.load(Ordering::Acquire) == self.config_generation
    }
}

pub(crate) fn filter_addresses(
    domain: &str,
    addresses: Vec<IpAddress>,
    family: AddressFamily,
) -> io::Result<Vec<IpAddress>> {
    let addresses: Vec<_> = addresses
        .into_iter()
        .map(|address| match canonicalize_ip(crate::ip_address_to_std(address)) {
            IpAddr::V4(ip) => IpAddress::V4(ip.octets()),
            IpAddr::V6(ip) => IpAddress::V6(ip.octets()),
        })
        .filter(|address| family.allows(crate::ip_address_to_std(*address)))
        .collect();
    if addresses.is_empty() && family != AddressFamily::Auto {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("DNS name `{domain}` has no addresses allowed by {family:?}"),
        ));
    }
    Ok(addresses)
}

#[cfg(test)]
mod tests;
