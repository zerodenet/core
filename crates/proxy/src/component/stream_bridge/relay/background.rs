//! Rebuild a selected prefix from the actual host's current role generations.
use super::*;
use crate::component::instance::invalid;
use zero_core::request::{ServiceOrigin, ServicePurpose, ServiceRequestFacts};
use zero_platform_tokio::contract::{context::RequestIdentity, network::CarrierContext};

pub(crate) struct BackgroundPath<'a> {
    pub(crate) nodes: Vec<crate::inventory::service::ServiceNode>,
    pub(crate) binding: UpstreamBinding,
    pub(crate) endpoint: Destination,
    pub(crate) wire_endpoint: Destination,
    pub(crate) final_tag: &'a str,
    pub(crate) call: CarrierContext,
}
impl BackgroundPath<'_> {
    pub(crate) async fn select(
        self,
        services: &TcpExecutionServices,
    ) -> Result<UpstreamTransport, ChannelError> {
        self.select_observed(services, None).await
    }
    pub(crate) async fn select_observed(
        self,
        services: &TcpExecutionServices,
        waiting: Option<zero_platform_tokio::contract::network::PendingCarrierObserver>,
    ) -> Result<UpstreamTransport, ChannelError> {
        self.select_observed_with_identity(services, waiting, None)
            .await
    }
    pub(crate) async fn select_observed_with_identity(
        self,
        services: &TcpExecutionServices,
        waiting: Option<zero_platform_tokio::contract::network::PendingCarrierObserver>,
        identity: Option<RequestIdentity>,
    ) -> Result<UpstreamTransport, ChannelError> {
        if identity
            .as_ref()
            .is_some_and(|identity| !matches!(identity, RequestIdentity::Service(_)))
        {
            return Err(invalid(
                Reason::StalePlan,
                "packet driver Service facts changed",
            ));
        }
        let Self {
            nodes,
            binding,
            endpoint,
            wire_endpoint,
            final_tag,
            call,
        } = self;
        if call.session.is_some()
            || binding.prefix.is_empty()
            || binding.network_generation.0 != services.upstream().network_generation()
        {
            return Err(invalid(
                Reason::StalePlan,
                "background path has no actual current prefix",
            ));
        }
        if nodes.len() != binding.prefix.len()
            || nodes
                .iter()
                .zip(&binding.prefix)
                .any(|(node, role)| node.slot.role_id() != *role)
        {
            return Err(invalid(
                Reason::StalePlan,
                "background prefix role ownership changed",
            ));
        }
        let end = call
            .deadline
            .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(5));
        let first = &nodes[0];
        let root_binding = UpstreamBinding {
            prefix: vec![],
            dial: first.dial.clone(),
            network_generation: binding.network_generation,
        };
        let mut upstream = match &first.endpoint {
            Some(endpoint) => UpstreamTransport::new(
                root_binding.clone(),
                Arc::new(
                    NodeOpener::new(
                        services.upstream_observed(first.tag.as_deref().unwrap_or("")),
                        endpoint.clone(),
                    )
                    .with_plan(services.components.as_ref().ok().map(Arc::downgrade))
                    .with_pending(root_binding, waiting)
                    .with_service_call(call.clone()),
                ),
            ),
            None => {
                let plan = services.components.as_ref().map_err(|source| {
                    invalid(Reason::StalePlan, "Packet prefix host retired")
                        .with_source(source.clone())
                })?;
                if first.slot.recipe.spec().resources.iter().any(|resource|matches!(resource,
                    zero_platform_tokio::contract::component::ResourceDeclaration::ControlledDirectStream { .. })) {
                    let target = nodes.get(1).and_then(|node|node.endpoint.clone()).unwrap_or_else(||endpoint.clone());
                    crate::component::direct_bridge::background::Root::transport(
                        &first.slot,Arc::downgrade(plan),services.upstream_observed(first.tag.as_deref().unwrap_or("")),
                        target,root_binding,crate::component::direct_bridge::background::Caller {business:None,call:Some(call.clone()),waiting},
                    )?.1
                } else {
                if !plan.has_device_carrier_provider(first.slot.role_id()) {
                    return Err(invalid(
                        Reason::Revoked,
                        "Packet prefix actual source is not ready",
                    ));
                }
                super::super::packet_network::service_transport(
                    services.upstream(),
                    root_binding,
                    waiting,
                    call.clone(),
                )?
                }
            }
        };
        for (index, node) in nodes.iter().enumerate() {
            if index != 0 && node.slot.recipe.spec().resources.iter().any(|resource| matches!(resource,
                zero_platform_tokio::contract::component::ResourceDeclaration::ControlledCarrierPool { .. })) {
                services
                    .components
                    .as_ref()
                    .map_err(|source| {
                        invalid(Reason::StalePlan, "prefix has no actual host")
                            .with_source(source.clone())
                    })?
                    .authorize_carrier_prefix(node.slot.instance_id(), &binding.prefix[..index])?;
            }
            let (next, wire, dial, tag) = if let Some(next) = nodes.get(index + 1) {
                let next_endpoint = next.endpoint.as_ref().ok_or_else(|| {
                    invalid(
                        Reason::Unsupported,
                        "background prefix requires endpoint facts",
                    )
                })?;
                (
                    next_endpoint.clone(),
                    next.wire_endpoint
                        .clone()
                        .unwrap_or_else(|| next_endpoint.clone()),
                    next.dial.clone(),
                    next.tag.as_deref().unwrap_or(""),
                )
            } else {
                // Preserve the existing named-target wire form for relay endpoints.
                let wire = wire_endpoint.clone();
                (endpoint.clone(), wire, binding.dial.clone(), final_tag)
            };
            let prefix = binding.prefix[..=index].to_vec();
            let selected = UpstreamBinding {
                prefix,
                dial,
                network_generation: binding.network_generation,
            };
            let context = OpenContext {
                identity: identity.clone().unwrap_or_else(|| RequestIdentity::Service(Arc::new(ServiceRequestFacts {
                    origin: ServiceOrigin::Runtime,
                    purpose: ServicePurpose::BackgroundControl,
                }).into())),
                deadline: call.deadline,
                cancel: call.cancel.clone(),
                upstream,
                observer: if index == 0 && node.endpoint.is_none() && !node.slot.recipe.spec().resources.iter().any(|resource|matches!(resource,zero_platform_tokio::contract::component::ResourceDeclaration::ControlledDirectStream {..})) {
                    let tag = node.tag.as_deref().ok_or_else(|| {
                        invalid(
                            Reason::StalePlan,
                            "Packet prefix has no actual configured tag",
                        )
                    })?;
                    Some(Arc::new(super::super::packet_network::PacketObserver {
                        engine: services.engine().clone(),
                        tag: tag.into(),
                        session: None,
                        traffic: super::super::traffic::EstablishmentTraffic::new(None),
                        inner: services.outbound_io(tag, zero_api::TrafficPlane::Inner),
                    }))
                } else {
                    None
                },
            };
            let role = node.slot.stream(end).await?;
            let mut opener = PrefixOpener::for_service(
                role,
                context,
                selected.clone(),
                next,
                wire,
                services.upstream(),
                if tag.is_empty() {
                    None
                } else {
                    services.outbound_io(tag, zero_api::TrafficPlane::Outer)
                },
            );
            opener.service_identity = identity.clone();
            opener.plan = services.components.as_ref().ok().map(Arc::downgrade);
            opener.datagram_slot = Some(node.slot.clone());
            opener.health = (index == 0).then(|| {
                FirstHopHealth::new(
                    services.engine(),
                    &services.snapshot_handle(),
                    node.tag.as_deref().unwrap_or(""),
                )
            });
            upstream = UpstreamTransport::new(selected, Arc::new(opener));
        }
        Ok(upstream)
    }
}
