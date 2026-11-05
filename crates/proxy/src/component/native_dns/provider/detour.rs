//! The existing selected Stream roles transfer real loans into the DNS owner.
use super::*;
use crate::{
    inventory::service::{ServicePlan, ServiceSelections},
    runtime::tcp_dispatch::service::{NativeDnsBinding, ServiceBinding},
};
use zero_platform_tokio::contract::network::UpstreamTransport;
mod root;

impl Provider {
    pub(super) fn detour_admitted(&self, outbound: &str) -> bool {
        let Ok(source) = self.network.current_services() else {
            return false;
        };
        let Ok(services) = source.runtime_services() else {
            return false;
        };
        let Some(id) = services.snapshot().plan().target_id(outbound) else {
            return false;
        };
        let execution = services.execution();
        let Ok(plan) = execution.components.as_ref() else {
            return false;
        };
        services
            .protocols()
            .carrier_service_target(services.snapshot(), id, plan)
    }

    pub(super) async fn open_detour(
        &self,
        outbound: String,
        peer: SocketAddr,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: tokio::time::Instant,
    ) -> std::io::Result<(StreamLoan, Arc<dyn zero_dns::DnsCarrierBinding>)> {
        let source = self.network.current_services()?;
        let services = source.runtime_services()?;
        let execution = services.execution();
        let components = execution
            .components
            .as_ref()
            .map_err(|source| std::io::Error::other(source.clone()))?;
        let target = services
            .snapshot()
            .plan()
            .target_id(&outbound)
            .ok_or_else(|| std::io::Error::other("DNS detour target retired"))?;
        if !services
            .protocols()
            .carrier_service_target(services.snapshot(), target, components)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "DNS detour has no complete original carrier path",
            ));
        }
        let selections = ServiceSelections::capture(services.snapshot(), target)
            .ok_or_else(|| std::io::Error::other("DNS detour selection cannot be captured"))?;
        let (resolved, _) = services
            .engine()
            .resolve_target_id_in_snapshot(services.snapshot(), target)
            .ok_or_else(|| std::io::Error::other("DNS detour cannot be resolved"))?;
        let prepared = crate::runtime::tcp_dispatch::service::prepare(
            services.protocols(),
            &services,
            &resolved,
        )
        .map_err(|source| std::io::Error::other(source.error))?
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "DNS detour Stream role unavailable",
            )
        })?;
        let branches = match prepared {
            ServicePlan::Single(node) => vec![vec![node]],
            ServicePlan::Relay(nodes) => vec![nodes],
            ServicePlan::Fallback(nodes) => nodes.into_iter().map(|node| vec![node]).collect(),
        };
        let mut last = None;
        for nodes in branches {
            let binding = UpstreamBinding {
                prefix: nodes.iter().map(|node| node.slot.role_id()).collect(),
                dial: Default::default(),
                network_generation: zero_core::component::NetworkGeneration(
                    self.egress.generation(),
                ),
            };
            let capture = ServiceBinding::new(
                &nodes,
                destination(peer),
                binding.network_generation,
                execution.upstream().network_updates(),
            )
            .map_err(std::io::Error::other)?;
            let selected = selections
                .chosen(&capture.tags())
                .ok_or_else(|| std::io::Error::other("DNS detour selected branch is ambiguous"))?;
            let receipt = Arc::new(NativeDnsBinding {
                carrier: capture,
                selections: selected,
            }) as Arc<dyn zero_dns::DnsCarrierBinding>;
            let call = CarrierContext {
                session: None,
                deadline: Some(end),
                cancel: cancel.clone(),
            };
            let endpoint = nodes[0]
                .endpoint
                .clone()
                .unwrap_or_else(|| destination(peer));
            let dial = &nodes[0].dial;
            let root_grants = self
                .roots
                .iter()
                .find(|(root, _)| root.endpoint == endpoint && root.dial == *dial)
                .map(|(_, grants)| grants)
                .or_else(|| {
                    self.peers
                        .iter()
                        .find(|(address, packet, _)| {
                            !packet
                                && destination(*address) == endpoint
                                && *dial == Default::default()
                        })
                        .map(|(_, _, grants)| grants)
                })
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "DNS root was not declared in this actual assembly",
                    )
                })?;
            let root_binding = UpstreamBinding {
                prefix: vec![],
                dial: dial.clone(),
                network_generation: binding.network_generation,
            };
            let network = nodes[0].tag.as_deref().map_or_else(
                || execution.upstream(),
                |tag| execution.upstream_observed(tag),
            );
            let root = Arc::new(
                root::Root::new(
                    network,
                    endpoint,
                    root_binding.clone(),
                    root_grants.clone(),
                    self.users.clone(),
                    caller.clone(),
                    cancel.clone(),
                )
                .map_err(std::io::Error::other)?,
            );
            components
                .authorize_carrier_prefix(self.instance, &binding.prefix)
                .map_err(std::io::Error::other)?;
            // The physical original already belongs to this DNS owner. Only
            // borrow the current admission fence; do not manufacture another
            // carrier slot around its own retained stream.
            let epoch = self.admit(&caller, &cancel, end, 1).await?;
            drop(epoch);
            let mut retry = false;
            let operation = async {
                let io = crate::runtime::tcp_dispatch::service::connect_stream(
                    &execution,
                    &destination(peer),
                    &nodes,
                    zero_core::request::ServicePurpose::DnsQuery,
                    &call,
                    UpstreamTransport::new(root_binding, root.clone()),
                )
                .await
                .map_err(|source| {
                    retry = source.retry_permitted();
                    std::io::Error::other(source)
                })?;
                Ok((root.loan(io).await?, receipt))
            }
            .await;
            match operation {
                Ok(loan) => return Ok(loan),
                Err(opening) => {
                    if let Err(cleanup) = self.close_attempt_originals(&caller, end).await {
                        return Err(std::io::Error::new(
                            cleanup.kind(),
                            custody::OpenCleanupFailure { opening, cleanup },
                        ));
                    }
                    if !retry || cancel.is_cancelled() || tokio::time::Instant::now() >= end {
                        return Err(opening);
                    }
                    last = Some(opening);
                }
            }
        }
        Err(last
            .unwrap_or_else(|| std::io::Error::other("DNS detour has no executable candidates")))
    }
}
