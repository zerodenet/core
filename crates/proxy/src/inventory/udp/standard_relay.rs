//! Select actual per-flow Datagram roles over composable ordinary Stream roles.
use crate::{
    inventory::service::ServiceNode,
    protocol_registry::{OutboundAdapterContext, UdpAdapterContext},
};
use zero_engine::ResolvedLeafOutbound;
use zero_platform_tokio::contract::component::{ResourceDeclaration, RoleKind};
pub(crate) struct PreparedDatagramRelay {
    pub(crate) prefix: Vec<ServiceNode>,
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
        if !final_slot
            .recipe
            .spec()
            .resources
            .iter()
            .any(|resource| matches!(resource, ResourceDeclaration::ControlledNodeStream { .. }))
        {
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
            if node.endpoint.is_none()
                || node.dial != Default::default()
                || !spec.roles.contains(&RoleKind::Stream)
                || spec.resources.iter().any(|resource| {
                    !matches!(
                        resource,
                        ResourceDeclaration::File { .. }
                            | ResourceDeclaration::ControlledCarrierPool {
                                datagram: false,
                                ..
                            }
                    )
                })
            {
                return None;
            }
            if spec.task_limit.is_some() || !plan.stream_dependencies_supported(node.slot.role_id())
            {
                return None;
            }
            if !spec.dependencies.is_empty() && plan.has_carrier_provider(node.slot.role_id()) {
                borrowed = Some(nodes.len());
            } else if borrowed.is_none()
                && spec
                    .resources
                    .iter()
                    .any(|r| matches!(r, ResourceDeclaration::ControlledCarrierPool { .. }))
            {
                borrowed = Some(nodes.len());
            }
            nodes.push(node);
        }
        if final_slot.dial != Default::default() {
            return None;
        }
        if matches!(
            nodes[0].endpoint.as_ref()?.address,
            zero_core::Address::Domain(_)
        ) && zero_dns::native::validate_node_config(ctx.config().runtime.dns.as_ref()).is_err()
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
            borrowed,
            target,
        }))
    }
}
