//! Sealed host-selected relay acquisition; ordinary native roots remain strict.
use super::*;
use crate::contract::dns::{DnsGeneration, DnsService};
impl NativeCarrierOpener {
    pub fn business_relay(
        grant: super::super::CarrierBusinessGrant,
        root: Destination,
        peer: SocketAddr,
        dns: Option<(Arc<dyn DnsService>, DnsGeneration)>,
        egress: Arc<EgressInterfaceControl>,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Result<Self, ChannelError> {
        grant.check_liveness()?;
        let fail = || {
            error(
                grant.binding().registration().instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            )
        };
        if !grant.is_relay() || grant.upstream().prefix.is_empty() || root.port != peer.port() {
            return Err(fail());
        }
        let matches = match root.address {
            Address::Ipv4(ip) => peer.ip() == std::net::IpAddr::from(ip),
            Address::Ipv6(ip) => peer.ip() == std::net::IpAddr::from(ip),
            Address::Domain(_) => dns
                .as_ref()
                .is_some_and(|(service, generation)| service.generation() == *generation),
        };
        if !matches {
            return Err(fail());
        }
        let mut binding = grant.upstream().clone();
        binding.prefix.clear();
        let mut native = Self::new(
            grant.binding().registration().instance(),
            binding,
            peer,
            egress,
            observer,
        )?;
        native.binding = grant.upstream().clone();
        native.endpoint = grant.endpoint().clone();
        native.business = Some(grant);
        native.dns = dns;
        Ok(native)
    }
}
