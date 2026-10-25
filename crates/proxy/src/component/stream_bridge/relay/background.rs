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
        let first_endpoint = first.endpoint.as_ref().ok_or_else(|| {
            invalid(
                Reason::Unsupported,
                "background prefix requires an actual endpoint role",
            )
        })?;
        let root = Arc::new(NodeOpener::new(
            services.upstream_observed(first.tag.as_deref().unwrap_or("")),
            first_endpoint.clone(),
        ));
        let mut upstream = UpstreamTransport::new(
            UpstreamBinding {
                prefix: vec![],
                dial: first.dial.clone(),
                network_generation: binding.network_generation,
            },
            root,
        );
        for (index, node) in nodes.iter().enumerate() {
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
                identity: RequestIdentity::Service(Arc::new(ServiceRequestFacts {
                    origin: ServiceOrigin::Runtime,
                    purpose: ServicePurpose::BackgroundControl,
                })),
                deadline: call.deadline,
                cancel: call.cancel.clone(),
                upstream,
                observer: None,
            };
            let role = node.slot.stream(end).await?;
            let mut opener = PrefixOpener::for_service(
                role,
                context,
                selected.clone(),
                next,
                wire,
                services.upstream(),
                services.outbound_io(tag, zero_api::TrafficPlane::Outer),
            );
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
