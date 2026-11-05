//! System resolver with retained original blocking lookup ownership.

use std::io;
use std::net::IpAddr;

use zero_traits::{AddressFamily, DnsResolver, IpAddress};
mod owner;

/// One retained owner for the actual OS resolver jobs, without claiming to own
/// the resolver service's private sockets or system configuration.
#[derive(Debug, Clone, Default)]
pub(crate) struct TokioSystemResolver(std::sync::Arc<owner::LookupOwner>);

impl DnsResolver for TokioSystemResolver {
    type Error = io::Error;

    async fn resolve(&self, domain: &str) -> Result<Vec<IpAddress>, Self::Error> {
        if crate::message::normalize_domain(domain)
            .is_ok_and(|name| crate::localhost::is_localhost(&name))
        {
            return Ok(crate::localhost::addresses(Default::default()));
        }
        self.0.resolve(domain.to_owned()).await
    }
}

impl TokioSystemResolver {
    pub(crate) async fn resolve_type(
        &self,
        domain: &str,
        query_type: u16,
        family: AddressFamily,
    ) -> io::Result<Vec<IpAddress>> {
        let addresses = self.resolve(domain).await?;
        let addresses = crate::family::filter_addresses(domain, addresses, family)?;
        Ok(addresses
            .into_iter()
            .filter(|address| {
                matches!(
                    (query_type, address),
                    (crate::message::TYPE_A, IpAddress::V4(_))
                        | (crate::message::TYPE_AAAA, IpAddress::V6(_))
                )
            })
            .collect())
    }
    pub(crate) fn revoke(&self) {
        self.0.revoke();
    }
    pub(crate) fn is_open(&self) -> bool {
        self.0.is_open()
    }
    pub(crate) fn externally_owned(&self) -> bool {
        std::sync::Arc::strong_count(&self.0) > 1
    }
    pub(crate) fn retained(&self) -> bool {
        self.0.retained()
    }
    pub(crate) fn close_idle(&self) -> bool {
        self.0.close_idle()
    }
    pub(crate) fn reopen_idle(&self) {
        self.0.reopen_idle();
    }
    pub(crate) async fn shutdown(&self, end: tokio::time::Instant) -> io::Result<()> {
        self.0.shutdown(end).await
    }
}

fn ip_addr_to_ip(addr: IpAddr) -> IpAddress {
    match addr {
        IpAddr::V4(v4) => IpAddress::V4(v4.octets()),
        IpAddr::V6(v6) => IpAddress::V6(v6.octets()),
    }
}

#[cfg(test)]
mod tests;
