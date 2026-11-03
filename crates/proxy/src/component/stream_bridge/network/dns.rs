//! One-demand DNS candidates selected through the actual native Node owner.
use super::*;
pub(super) struct Resolved {
    resolver: Arc<dyn zero_platform_tokio::contract::dns::DnsService>,
    generation: zero_platform_tokio::contract::dns::DnsGeneration,
    peers: Vec<SocketAddr>,
    next: usize,
}
impl NodeOpener {
    pub(super) fn prepare_node_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
        grant: CarrierGrant,
    ) -> BoxFuture<'_, Result<PreparedCarrier, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            if !matches!(destination.address, zero_core::Address::Domain(_)) {
                return self.prepare_controlled_stream(&binding, destination, context, grant);
            }
            if destination != self.endpoint
                || !binding.prefix.is_empty()
                || binding.network_generation.0 != self.generation
                || self.services.network_generation() != self.generation
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "node DNS cannot replace endpoint, prefix or network",
                ));
            }
            let request = self.business.as_ref().ok_or_else(|| {
                invalid(
                    Reason::Unsupported,
                    "node DNS requires actual business admission",
                )
            })?;
            let ticket = request.authorize(&destination, &binding, &grant)?;
            let mut resolved = self.resolved.lock().await;
            if resolved.is_none() {
                use zero_platform_tokio::contract::dns::{
                    DnsQueryContext, DnsQueryRole, DnsRequest,
                };
                let resolver = self.services.native_node_dns();
                let generation = resolver.generation();
                let zero_core::Address::Domain(name) = &destination.address else {
                    unreachable!()
                };
                let mut query = DnsRequest::new(name, DnsQueryRole::Node);
                query.family = binding.dial.effective_family().map_err(|e| {
                    invalid(Reason::InvalidConfig, "invalid node dial policy").with_source(e)
                })?;
                let answer = resolver
                    .resolve(
                        query,
                        DnsQueryContext {
                            deadline: context.deadline,
                            cancel: context.cancel.clone(),
                            expected_generation: Some(generation),
                        },
                    )
                    .await?;
                if answer.generation != generation {
                    return Err(invalid(Reason::StalePlan, "node DNS generation changed"));
                }
                let peers = answer
                    .addresses
                    .into_iter()
                    .map(|ip| {
                        SocketAddr::new(
                            match ip {
                                zero_traits::IpAddress::V4(ip) => {
                                    std::net::Ipv4Addr::from(ip).into()
                                }
                                zero_traits::IpAddress::V6(ip) => {
                                    std::net::Ipv6Addr::from(ip).into()
                                }
                            },
                            destination.port,
                        )
                    })
                    .collect();
                *resolved = Some(Resolved {
                    resolver,
                    generation,
                    peers,
                    next: 0,
                });
            }
            let resolved = resolved.as_mut().unwrap();
            let peer = *resolved
                .peers
                .get(resolved.next)
                .ok_or_else(|| invalid(Reason::Io, "node DNS candidates exhausted"))?;
            resolved.next += 1;
            NativeCarrierOpener::business_resolved(
                ticket,
                peer,
                resolved.resolver.clone(),
                resolved.generation,
                Arc::new(self.services.egress_control()),
                self.services.observer(),
            )?
            .prepare_controlled_stream(&binding, destination, context, grant)
            .map(|prepared| {
                prepared.with_endpoint_observer(Arc::new(ConnectedEvidence(self.evidence.clone())))
            })
        })
    }
}
