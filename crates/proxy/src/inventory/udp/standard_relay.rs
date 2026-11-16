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
        let target =
            match super::association::prepare_flow(ctx, last, zero_engine::RouteMode::Flow)? {
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
        // A standard receiver is never reinterpreted as a compatibility pool.
        // Select the same actual installed original graph used by Stream loans;
        // its protocol and Host keep physical custody through all UDP callers.
        let pooled = !node_business && plan.has_stream_loan_provider(final_slot.role_id());
        if (node_business && !plan.has_node_stream_provider(final_slot.role_id()))
            || (!node_business && !pooled)
            || !plan.stream_dependencies_supported(final_slot.role_id())
        {
            return Some(Err(failure(
                std::io::Error::other(crate::component::instance::invalid(
                    zero_platform_tokio::contract::Reason::Revoked,
                    "standard Datagram receiver lacks its actual original provider",
                ))
                .into(),
            )));
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
                if node.slot.accepted_carrier_provider().is_some() && nodes.is_empty() {
                    borrowed = Some(nodes.len());
                    nodes.push(node);
                    continue;
                }
                let direct = spec.resources.iter().any(|resource| {
                    matches!(
                        resource,
                        ResourceDeclaration::ControlledDirectStream { .. }
                            | ResourceDeclaration::ControlledDirectCarrier { .. }
                    )
                });
                if direct && nodes.is_empty() {
                    if !plan.has_stream_loan_provider(node.slot.role_id()) {
                        return Some(Err(failure(
                            std::io::Error::other(crate::component::instance::invalid(
                                zero_platform_tokio::contract::Reason::Revoked,
                                "Direct prefix lacks its original native provider",
                            ))
                            .into(),
                        )));
                    }
                    borrowed = Some(nodes.len());
                    nodes.push(node);
                    continue;
                }
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
            if !spec.roles.contains(&RoleKind::Stream)
                || !plan.stream_dependencies_supported(node.slot.role_id())
            {
                return Some(Err(failure(
                    std::io::Error::other(crate::component::instance::invalid(
                        zero_platform_tokio::contract::Reason::Unsupported,
                        "selected Datagram prefix has no supported standard Stream loan path",
                    ))
                    .into(),
                )));
            }
            if plan.has_stream_loan_provider(node.slot.role_id()) {
                // Borrow the complete prefix through its last actual owner.
                // A per-connection Node original is not an inline framing-only
                // hop: it transfers the same authentic StreamLoan as a pool.
                borrowed = Some(nodes.len());
            } else if spec.task_limit.is_some() {
                return Some(Err(failure(
                    std::io::Error::other(crate::component::instance::invalid(
                        zero_platform_tokio::contract::Reason::Revoked,
                        "selected prefix task owner has no installed original provider",
                    ))
                    .into(),
                )));
            }
            nodes.push(node);
        }
        if nodes[0]
            .endpoint
            .as_ref()
            .is_some_and(|endpoint| matches!(endpoint.address, zero_core::Address::Domain(_)))
            && crate::adapters::node_dns::validate(ctx.config()).is_err()
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
