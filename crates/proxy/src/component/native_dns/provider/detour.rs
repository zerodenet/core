//! The existing selected Stream roles transfer real loans into the DNS owner.
use super::*;
use crate::{
    inventory::service::{ServicePlan, ServiceSelections},
    runtime::tcp_dispatch::service::{NativeDnsBinding, ServiceBinding},
};
use zero_platform_tokio::contract::network::UpstreamTransport;
mod borrowed;
mod datagram;
enum Detour {
    Stream(StreamLoan, Arc<dyn zero_dns::DnsCarrierBinding>),
    Datagram(DatagramLoan),
}
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
        match self
            .open_selected_detour(outbound, peer, caller, cancel, end, false)
            .await?
        {
            Detour::Stream(loan, binding) => Ok((loan, binding)),
            Detour::Datagram(_) => unreachable!("a Stream request does not select a datagram"),
        }
    }
    pub(super) async fn open_udp_detour(
        &self,
        outbound: String,
        peer: SocketAddr,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: tokio::time::Instant,
    ) -> std::io::Result<zero_dns::native::NativeDnsUdpLoan> {
        match self
            .open_selected_detour(outbound, peer, caller, cancel, end, true)
            .await?
        {
            Detour::Stream(loan, _) => Ok(zero_dns::native::NativeDnsUdpLoan::Stream(loan)),
            Detour::Datagram(loan) => Ok(zero_dns::native::NativeDnsUdpLoan::Datagram(loan)),
        }
    }
    async fn open_selected_detour(
        &self,
        outbound: String,
        peer: SocketAddr,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: tokio::time::Instant,
        udp: bool,
    ) -> std::io::Result<Detour> {
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
        let selections = ServiceSelections::capture(services.snapshot(), target)
            .ok_or_else(|| std::io::Error::other("DNS detour selection cannot be captured"))?;
        let (resolved, _) = services
            .engine()
            .resolve_target_id_in_snapshot(services.snapshot(), target)
            .ok_or_else(|| std::io::Error::other("DNS detour cannot be resolved"))?;
        if udp {
            let datagram = services
                .protocols()
                .prepare_service_datagram(
                    crate::protocol_registry::OutboundAdapterContext::new(services.config()),
                    &resolved,
                    components,
                )
                .map_err(|failure| std::io::Error::other(failure.error))?;
            if let Some(node) = datagram {
                return self
                    .open_datagram_detour(
                        &execution,
                        node,
                        selections,
                        peer,
                        caller,
                        CarrierContext {
                            session: None,
                            deadline: Some(end),
                            cancel,
                        },
                    )
                    .await
                    .map(Detour::Datagram);
            }
        }
        if !services
            .protocols()
            .carrier_service_target(services.snapshot(), target, components)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "DNS detour has no complete original carrier path",
            ));
        }
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
            // A device Stream consumes its existing virtual socket/provider.
            // This only removes native dial authority: actual role/provider
            // admission remains with the current ComponentPlan and selection.
            let device_root = nodes[0].endpoint.is_none()
                && nodes[0].role != zero_platform_tokio::contract::dns::DnsQueryRole::Direct
                && nodes[0]
                    .slot
                    .recipe
                    .spec()
                    .roles
                    .contains(&zero_platform_tokio::contract::component::RoleKind::Packet);
            let root = root::Root::new(
                network,
                endpoint,
                root_binding.clone(),
                root_grants.clone(),
                self.budget.clone(),
                self.users.clone(),
                self.pending.clone(),
                caller.clone(),
                cancel.clone(),
                call.clone(),
                execution.current_source.clone(),
                binding.prefix.clone(),
                self.cleanup_receiver(),
            )
            .map_err(std::io::Error::other)?;
            let direct_endpoint = (nodes[0].endpoint.is_none()
                && nodes[0].role == zero_platform_tokio::contract::dns::DnsQueryRole::Direct)
                .then(|| {
                    nodes
                        .get(1)
                        .and_then(|node| node.endpoint.clone())
                        .unwrap_or_else(|| destination(peer))
                });
            let root = root.with_direct_endpoint(direct_endpoint);
            let root = Arc::new(if device_root {
                root.observer_only()
            } else {
                root
            });
            root.track();
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
                let last = nodes.last().expect("selected DNS Stream path");
                if components.has_carrier_provider(last.slot.role_id())
                    || components.has_device_carrier_provider(last.slot.role_id())
                {
                    // Device Stream roles lend their existing virtual socket.
                    // Their actual Bound/Listener original is not a native TCP
                    // pool; the observer-only DNS root must never invent one.
                    let loan = self
                        .borrow_detour(
                            &execution,
                            &nodes,
                            peer,
                            &binding,
                            &call,
                            caller.clone(),
                            UpstreamTransport::new(root_binding, root),
                        )
                        .await
                        .inspect_err(|source| {
                            retry = source
                                .get_ref()
                                .and_then(|source| source.downcast_ref::<zero_platform_tokio::contract::ChannelError>())
                                .is_some_and(|source| source.retry_permitted());
                        })?;
                    receipt.check_current()?;
                    return Ok(Detour::Stream(loan, receipt));
                }
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
                let loan = root.loan(io).await?;
                receipt.check_current()?;
                Ok(Detour::Stream(loan, receipt))
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
