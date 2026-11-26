//! Concrete projection of the existing System TUN ingress behavior.
use std::{
    net::IpAddr,
    sync::{atomic::AtomicU64, Arc},
};
#[derive(Clone)]
pub(crate) struct TunIngressOptions {
    pub(crate) addresses: Vec<(IpAddr, IpAddr)>,
    pub(crate) mtu: u16,
    pub(crate) dns_hijack: bool,
    pub(crate) dns_hijacked_queries: Arc<AtomicU64>,
    pub(crate) native_io: Arc<dyn zero_traits::IoObserver>,
}

impl TunIngressOptions {
    pub(crate) fn supports_family(&self, address: IpAddr) -> bool {
        self.addresses
            .iter()
            .any(|(configured, _)| configured.is_ipv4() == address.is_ipv4())
    }

    pub(crate) fn permits_routed_destination(
        &self,
        destination: IpAddr,
        local_destination: bool,
    ) -> bool {
        if !self.supports_family(destination)
            || local_destination
            || destination.is_unspecified()
            || destination.is_multicast()
        {
            return false;
        }
        match destination {
            IpAddr::V4(ipv4) => {
                if ipv4.is_broadcast() || ipv4.is_link_local() {
                    return false;
                }
                for (configured, mask) in &self.addresses {
                    if let (IpAddr::V4(addr), IpAddr::V4(netmask)) = (*configured, *mask) {
                        let mask_bits = u32::from(netmask);
                        if mask_bits < 0xffff_fffe {
                            let dst_bits = u32::from(ipv4);
                            let net_bits = u32::from(addr) & mask_bits;
                            if (dst_bits & mask_bits) == net_bits
                                && (dst_bits | mask_bits) == u32::MAX
                            {
                                return false;
                            }
                        }
                    }
                }
                true
            }
            IpAddr::V6(ipv6) => !ipv6.is_unicast_link_local(),
        }
    }
}

#[cfg(test)]
#[path = "tests/policy.rs"]
mod tests;
