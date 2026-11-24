//! Installed device and metadata owners lend their actual standard datagram role.
use super::*;
use zero_platform_tokio::contract::component::RoleKind;

pub(crate) struct ServiceDatagram {
    pub(crate) slot: Arc<ComponentSlot>,
    pub(crate) tag: Option<String>,
    pub(crate) direct: bool,
    pub(crate) native: Option<(usize, zero_core::request::Destination)>,
    pub(crate) relay: Vec<ServiceNode>,
}
impl ProtocolInventory {
    pub(crate) fn prepare_service_datagram(
        &self,
        ctx: OutboundAdapterContext<'_>,
        resolved: &ResolvedOutbound<'_>,
        components: &ComponentPlan,
    ) -> Result<Option<ServiceDatagram>, TcpOutboundFailure> {
        match resolved {
            ResolvedOutbound::Single(leaf) => self.service_datagram_leaf(ctx, leaf, components),
            ResolvedOutbound::Relay { chain } => {
                let Some(tail) = chain.last() else {
                    return Ok(None);
                };
                let Some(mut node) = self.service_datagram_leaf(ctx, tail, components)? else {
                    return Ok(None);
                };
                // Reuse the existing qualified Service prefix to reach a
                // self-owned cached/Metadata Datagram. Native Datagram roots
                // still need their exact single-role admission.
                let role = node.slot.role_id();
                if !(components.has_metadata_stream_provider(role)
                    || components.has_cached_stream_provider(role))
                    || node.native.is_some()
                    || node.direct
                {
                    return Ok(None);
                }
                let Some(ServicePlan::Relay(nodes)) =
                    self.prepare_service_stream(ctx, resolved, components)?
                else {
                    return Ok(None);
                };
                if nodes.first().is_none_or(|first| first.endpoint.is_none()) {
                    return Ok(None);
                }
                node.relay = nodes;
                Ok(Some(node))
            }
            ResolvedOutbound::Fallback { .. } => Ok(None),
        }
    }

    fn service_datagram_leaf(
        &self,
        ctx: OutboundAdapterContext<'_>,
        leaf: &zero_engine::ResolvedLeafOutbound<'_>,
        components: &ComponentPlan,
    ) -> Result<Option<ServiceDatagram>, TcpOutboundFailure> {
        if let zero_engine::ResolvedLeafOutbound::Direct { dial_policy, .. } = leaf {
            let Some(node) = self.service_node(ctx, leaf, components)? else {
                return Ok(None);
            };
            let spec = node.slot.recipe.spec();
            let installed = spec.roles.contains(&RoleKind::Datagram)
                && matches!(spec.resources.as_slice(), [zero_platform_tokio::contract::component::ResourceDeclaration::ControlledDirectCarrier { dial, max_sockets }]
                    if dial == dial_policy && node.slot.original_carrier_owner(0).is_some_and(|owner| owner.instance() == node.slot.instance_id() && owner.limit() == *max_sockets && owner.bound().is_some_and(|bound| node.slot.matches_live_scope(&bound.scope_identity()))));
            return Ok(installed.then_some(ServiceDatagram {
                slot: node.slot,
                tag: node.tag,
                native: None,
                direct: true,
                relay: Vec::new(),
            }));
        }
        let zero_engine::ResolvedLeafOutbound::Proxy { identity } = leaf else {
            return Ok(None);
        };
        let outbound = ctx
            .config()
            .outbounds
            .get(identity.config_index())
            .ok_or_else(|| TcpOutboundFailure {
                stage: "prepare_dns_datagram",
                error: zero_engine::EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "DNS datagram selected outbound retired",
                )),
                upstream_endpoint: None,
                network: None,
            })?;
        let Some(slot) = components.configured(&outbound.tag) else {
            return Ok(None);
        };
        let roles = &slot.recipe.spec().roles;
        let device = roles.contains(&RoleKind::Packet)
            && components.has_device_carrier_provider(slot.role_id());
        // This installed pair owns its actual native Metadata or borrowed
        // Datagram itself. Other Datagram roles may still require this Query
        // to supply their control Stream/UDP originals; an observer is not IO.
        let metadata = slot.dial == Default::default()
            && (components.has_metadata_stream_provider(slot.role_id())
                || components.has_cached_stream_provider(slot.role_id()))
            && components.stream_dependencies_supported(slot.role_id());
        // These roles publish no separate native originals. A component with
        // mixed ordinary and pooled roles keeps its existing Stream DNS path:
        // the presence of a pool does not prove its Datagram borrows that pool.
        let shared = slot.dial == Default::default()
            && slot.recipe.spec().task_limit.is_none()
            && (slot.recipe.spec().resources.is_empty()
                || slot.accepted_carrier_provider().is_some())
            && components.has_background_carrier_pool(slot.role_id())
            && components.stream_dependencies_supported(slot.role_id());
        // These self-owned donors already retain each physical half in their
        // background Root. Only the remaining SS/SOCKS shape asks this Query
        // to acquire its original: two cached H3 pools are not ambiguous native
        // issuers and must not fall back to Stream DNS framing.
        // Native SS/SOCKS use the selected recipe's complete policy. The
        // Query declares its logical endpoint with that same policy; the
        // actual donor still supplies its own original grants and Scope.
        let mut native = None;
        if !device
            && !metadata
            && !shared
            && components.stream_dependencies_supported(slot.role_id())
        {
            for (ordinal, resource) in slot.recipe.spec().resources.iter().enumerate() {
                use zero_platform_tokio::contract::component::ResourceDeclaration as R;
                let (endpoint, dial, capacity) = match resource {
                    R::ControlledCarrierPool {
                        endpoint,
                        dial,
                        datagram: true,
                        max_sockets,
                    }
                    | R::ControlledAssociationPool {
                        endpoint,
                        dial,
                        max_sockets,
                    } => (endpoint, dial, max_sockets),
                    _ => continue,
                };
                let installed = dial == &slot.dial
                    && slot.original_carrier_owner(ordinal).is_some_and(|owner| {
                        owner.instance() == slot.instance_id()
                            && owner.limit() == *capacity
                            && owner.bound().is_some_and(|bound| {
                                slot.matches_live_scope(&bound.scope_identity())
                            })
                    });
                if installed {
                    if native.is_some() {
                        return Ok(None);
                    }
                    native = Some((ordinal, endpoint.clone()));
                }
            }
        }
        if !roles.contains(&RoleKind::Datagram)
            || !(device || metadata || shared || native.is_some())
        {
            return Ok(None);
        }
        Ok(Some(ServiceDatagram {
            slot,
            tag: Some(outbound.tag.clone()),
            native,
            direct: false,
            relay: Vec::new(),
        }))
    }
}
