//! Immutable per-leaf requirements never modify shared egress state.
#[cfg(feature = "udp-runtime")]
use super::{direct_network_observation, DirectNetworkDialObservation, DirectTargetResolution};
use super::{socket_addr_from_ip, DirectConnector};
use std::net::SocketAddr;
use zero_core::{Address, Error, Session};
use zero_dns::DnsSystem;
use zero_engine::FlowNetworkObservation;
use zero_platform_tokio::EgressInterfaceControl;
use zero_traits::{AddressFamily, DialPolicy};

pub(super) fn filter_candidates(
    candidates: Vec<SocketAddr>,
    policy: &DialPolicy,
) -> Result<Vec<SocketAddr>, Error> {
    let mut permitted = Vec::new();
    for candidate in candidates {
        if let Ok(candidate) = policy.normalize_peer(candidate) {
            if !permitted.contains(&candidate) {
                permitted.push(candidate);
            }
        }
    }
    if permitted.is_empty() {
        return Err(Error::Io(
            "direct target has no address permitted by outbound dial policy",
        ));
    }
    Ok(permitted)
}
pub(super) fn family_name(policy: &DialPolicy, resolver: &DnsSystem) -> &'static str {
    match policy.effective_family() {
        Ok(AddressFamily::OnlyIpv4) => "only_ipv4",
        Ok(AddressFamily::OnlyIpv6) => "only_ipv6",
        _ => resolver.address_family_policy().as_str(),
    }
}
impl DirectConnector {
    pub(super) async fn resolve_addresses_with_policy(
        &self,
        address: &Address,
        port: u16,
        resolver: &DnsSystem,
        policy: &DialPolicy,
    ) -> Result<Vec<SocketAddr>, Error> {
        let candidates = match address {
            Address::Domain(domain) => {
                let family = policy
                    .effective_family()
                    .map_err(|_| Error::Config("invalid outbound dial family"))?;
                resolver.resolve_direct_with_family(domain, family).await.map_err(|error| {
                    tracing::debug!(domain, error = %error, "constrained direct resolution failed");
                    Error::Io("failed to resolve direct target")
                })?.into_iter().map(|ip| socket_addr_from_ip(ip, port)).collect()
            }
            _ => {
                self.resolve_addresses(address, port, resolver, "failed to resolve direct target")
                    .await?
            }
        };
        filter_candidates(candidates, policy)
    }
    pub(crate) fn resolution_failure_observation_with_policy(
        &self,
        session: &Session,
        resolver: &DnsSystem,
        egress: &EgressInterfaceControl,
        policy: &DialPolicy,
    ) -> FlowNetworkObservation {
        let mut observation = self.resolution_failure_observation(session, resolver, egress);
        observation.address_family_policy = Some(family_name(policy, resolver).to_owned());
        if policy.effective_family() == Ok(AddressFamily::OnlyIpv6) {
            observation.address_family_fallback = None;
        }
        observation
    }
    #[cfg(feature = "udp-runtime")]
    pub(crate) fn udp_network_observation_with_policy(
        &self,
        resolution: &DirectTargetResolution,
        remote: SocketAddr,
        local: Option<SocketAddr>,
        selection: &zero_platform_tokio::EgressSelection,
    ) -> FlowNetworkObservation {
        direct_network_observation(
            selection,
            DirectNetworkDialObservation {
                local,
                remote,
                resolved_candidates: &resolution.candidates,
                attempts: &[],
                connect_stage: "sent",
                interface_bound: selection.interface().is_some(),
            },
            resolution,
        )
    }
}
