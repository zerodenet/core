//! Select actual per-flow Datagram roles over composable ordinary Stream roles.
use crate::{
    inventory::service::ServiceNode,
    protocol_registry::{OutboundAdapterContext, UdpAdapterContext},
};
use zero_engine::ResolvedLeafOutbound;
use zero_platform_tokio::contract::component::{ResourceDeclaration, RoleKind};
pub(crate) struct PreparedDatagramRelay {
    pub(crate) prefix: Vec<ServiceNode>,
    pub(crate) pooled: bool,
    pub(crate) borrowed: Option<usize>,
    pub(crate) target: super::DatagramAssociationTarget,
}
impl super::super::ProtocolInventory {
    pub(super) fn prepare_standard_datagram_relay(
        &self,
        ctx: &UdpAdapterContext<'_>,
        chain: &[ResolvedLeafOutbound<'_>],
    ) -> Option<Result<PreparedDatagramRelay, crate::runtime::udp_dispatch::FlowFailure>> {
        let (last, prefix) = chain.split_last()?;
        if prefix.is_empty() {
            return None;
        }
        let execution = ctx.packet_path_execution_services().tcp();
        let failure = |error| crate::runtime::udp_dispatch::FlowFailure {
            stage: "udp_datagram_relay",
            error,
            upstream: None,
        };
        let plan = match execution.components.as_ref() {
            Ok(plan) => plan,
            Err(source) => {
                return Some(Err(failure(
                    std::io::Error::other(crate::component::instance::retained(source.clone()))
                        .into(),
                )))
            }
        };
        let target = match super::association::prepare_flow(ctx, last)? {
            Ok(target) => target,
            Err(error) => return Some(Err(error)),
        };
        let final_slot = match target.current_slot(&execution) {
            Ok(slot) => slot,
            Err(error) => return Some(Err(failure(error))),
        };
        let node_business =
            final_slot.recipe.spec().resources.iter().any(|resource| {
                matches!(resource, ResourceDeclaration::ControlledNodeStream { .. })
            });
        // An existing background pool must consume its own standard Datagram
        // role. Its persistent provider, not a legacy claim or this request,
        // retains and opens the actual physical carrier.
        let node_original =
            final_slot.recipe.spec().resources.iter().any(|resource| {
                matches!(resource, ResourceDeclaration::ControlledNodeCarrier { .. })
            }) && final_slot.recipe.spec().resources.iter().all(|resource| {
                matches!(
                    resource,
                    ResourceDeclaration::ControlledNodeCarrier { .. }
                        | ResourceDeclaration::File { .. }
                )
            });
        if node_original
            && !final_slot
                .recipe
                .spec()
                .resources
                .iter()
                .enumerate()
                .any(|(ordinal, resource)| match resource {
                    ResourceDeclaration::ControlledNodeCarrier { max_sockets, .. } => final_slot
                        .original_carrier_owner(ordinal)
                        .is_some_and(|owner| {
                            owner.instance() == final_slot.instance_id()
                                && owner.limit() == *max_sockets
                        }),
                    _ => false,
                })
        {
            return Some(Err(failure(
                std::io::Error::other(crate::component::instance::invalid(
                    zero_platform_tokio::contract::Reason::Revoked,
                    "canonical Datagram original provider is not ready",
                ))
                .into(),
            )));
        }
        let pooled = node_original
            || !node_business
                && final_slot.recipe.spec().roles.contains(&RoleKind::Stream)
                && final_slot.recipe.spec().task_limit.is_none()
                && !final_slot.recipe.spec().dependencies.is_empty()
                && final_slot.recipe.spec().resources.iter().all(|resource| {
                    matches!(
                        resource,
                        ResourceDeclaration::File { .. }
                            | ResourceDeclaration::ControlledNodeCarrier { .. }
                            | ResourceDeclaration::ControlledCarrierPool { datagram: true, .. }
                    )
                })
                && plan.has_background_carrier_pool(final_slot.role_id())
                && plan.stream_dependencies_supported(final_slot.role_id());
        if !node_business && !pooled {
            return None;
        }
        let mut nodes = Vec::new();
        let mut borrowed = None;
        for leaf in prefix {
            let node =
                match self.service_node(OutboundAdapterContext::new(ctx.config()), leaf, plan) {
                    Ok(Some(node)) => node,
                    Ok(None) => return None,
                    Err(error) => {
                        return Some(Err(crate::runtime::udp_dispatch::FlowFailure {
                            stage: error.stage,
                            error: error.error,
                            upstream: error.upstream_endpoint.map(|endpoint| *endpoint),
                        }))
                    }
                };
            let spec = node.slot.recipe.spec();
            if node.endpoint.is_none() {
                // This is the canonical device selected by the same host,
                // including while its original source is still starting.
                // A cold source must not cause a second legacy PacketFlow.
                if !nodes.is_empty()
                    || ![RoleKind::Packet, RoleKind::Stream, RoleKind::Datagram]
                        .iter()
                        .all(|role| spec.roles.contains(role))
                    || plan.device_component(node.slot.id).is_err()
                {
                    return None;
                }
                if !plan.has_device_carrier_provider(node.slot.role_id()) {
                    return Some(Err(failure(
                        std::io::Error::other(crate::component::instance::invalid(
                            zero_platform_tokio::contract::Reason::Revoked,
                            "Datagram prefix actual device source is not ready",
                        ))
                        .into(),
                    )));
                }
                borrowed = Some(nodes.len());
                nodes.push(node);
                continue;
            }
            if node.dial != Default::default()
                || !spec.roles.contains(&RoleKind::Stream)
                || spec.resources.iter().any(|resource| {
                    !matches!(
                        resource,
                        ResourceDeclaration::File { .. }
                            | ResourceDeclaration::ControlledCarrierPool { .. }
                            | ResourceDeclaration::ControlledNodeCarrier { .. }
                            | ResourceDeclaration::ControlledAssociationPool { .. }
                    )
                })
            {
                return None;
            }
            if (spec.task_limit.is_some()
                && !plan.has_preparing_node_carrier_provider(node.slot.role_id()))
                || !plan.stream_dependencies_supported(node.slot.role_id())
            {
                return None;
            }
            if (!spec.dependencies.is_empty() && plan.has_carrier_provider(node.slot.role_id()))
                || (borrowed.is_none()
                    && spec
                        .resources
                        .iter()
                        .any(|r| matches!(r, ResourceDeclaration::ControlledCarrierPool { .. })))
            {
                borrowed = Some(nodes.len());
            }
            nodes.push(node);
        }
        if final_slot.dial != Default::default() {
            return None;
        }
        if nodes[0]
            .endpoint
            .as_ref()
            .is_some_and(|endpoint| matches!(endpoint.address, zero_core::Address::Domain(_)))
            && zero_dns::native::validate_node_config(ctx.config().runtime.dns.as_ref()).is_err()
        {
            return None;
        }
        for leaf in chain {
            let tag = match leaf {
                ResolvedLeafOutbound::Proxy { identity } => ctx
                    .config()
                    .outbounds
                    .get(identity.config_index())
                    .map(|v| v.tag.as_str()),
                _ => None,
            };
            if !ctx.udp_enabled_for_outbound(tag) {
                return Some(Err(crate::runtime::udp_dispatch::FlowFailure {
                    stage: "udp_policy",
                    error: std::io::Error::other("udp disabled for relay chain outbound").into(),
                    upstream: None,
                }));
            }
        }
        Some(Ok(PreparedDatagramRelay {
            prefix: nodes,
            pooled,
            borrowed,
            target,
        }))
    }
}
