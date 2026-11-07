//! The actual NodeOpener performs this query on its original Native Node DNS.
use super::*;
impl ServiceDatagramTargetIssuer {
    /// Core Node producer only. The Host issuer's exact grant/Aux remains live
    /// through this original query; the candidate data adds no native rights.
    pub async fn resolve_on_node(
        self,
        source: &crate::controlled_carrier::loan::HostNodeDnsSource,
        current_network: NetworkGeneration,
    ) -> Result<BorrowedDatagramCandidates, ChannelError> {
        self.check()?;
        let instance = self.bound.registration().instance();
        if !self.bound.matches_host_cancel(&source.host_fence) {
            return Err(failure(instance, Reason::StalePlan));
        }
        let resolver = source.source.clone();
        if current_network != self.binding.network_generation {
            return Err(failure(instance, Reason::StalePlan));
        }
        let ip = match &self.endpoint.address {
            Address::Ipv4(ip) => Some(std::net::IpAddr::from(*ip)),
            Address::Ipv6(ip) => Some(std::net::IpAddr::from(*ip)),
            Address::Domain(name) => zero_core::address::parse_ip_literal(name),
        };
        let (peers, resolver) = match ip {
            Some(ip) => (
                CandidatePeers::Literal(SocketAddr::new(ip, self.endpoint.port)),
                None,
            ),
            None => {
                let Address::Domain(name) = &self.endpoint.address else {
                    unreachable!()
                };
                let generation = resolver.generation();
                if generation.network != current_network {
                    return Err(failure(instance, Reason::StalePlan));
                }
                let mut request = DnsRequest::new(name, DnsQueryRole::Node);
                request.family = self.binding.dial.effective_family().map_err(|source| {
                    failure(instance, Reason::InvalidConfig).with_source(source)
                })?;
                let response = resolver
                    .resolve(
                        request,
                        DnsQueryContext {
                            deadline: self.call.deadline,
                            cancel: self.call.cancel.clone(),
                            expected_generation: Some(generation),
                        },
                    )
                    .await?;
                if response.generation != generation || resolver.generation() != generation {
                    return Err(failure(instance, Reason::StalePlan));
                }
                (
                    CandidatePeers::Answer(response.addresses),
                    Some((resolver, generation)),
                )
            }
        };
        self.check()?;
        if peers.len() == 0 {
            return Err(failure(instance, Reason::Io));
        }
        Ok(BorrowedDatagramCandidates {
            bound: self.bound,
            endpoint: self.endpoint,
            binding: self.binding,
            identity: self.identity,
            call: self.call,
            peers,
            resolver,
        })
    }
}
