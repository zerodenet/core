//! Installed device and metadata owners lend their actual standard datagram role.
use super::*;
use zero_platform_tokio::contract::component::RoleKind;

pub(crate) struct ServiceDatagram {
    pub(crate) slot: Arc<ComponentSlot>,
    pub(crate) tag: Option<String>,
    pub(crate) native: Option<(usize, zero_core::request::Destination)>,
}
impl ProtocolInventory {
    pub(crate) fn prepare_service_datagram(
        &self,
        ctx: OutboundAdapterContext<'_>,
        resolved: &ResolvedOutbound<'_>,
        components: &ComponentPlan,
    ) -> Result<Option<ServiceDatagram>, TcpOutboundFailure> {
        // The old packet exchange accepted one leaf only. A resolved relay or
        // fallback keeps its existing Stream DNS grammar, without a second pick.
        let ResolvedOutbound::Single(zero_engine::ResolvedLeafOutbound::Proxy { identity }) =
            resolved
        else {
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
            && components.has_metadata_stream_provider(slot.role_id())
            && components.stream_dependencies_supported(slot.role_id());
        // The logical role publishes no native resources of its own. Its
        // installed background dependency already owns the shared physical
        // pool, including pending/cancelled setup and the actual driver.
        let shared = slot.dial == Default::default()
            && slot.recipe.spec().task_limit.is_none()
            && slot.recipe.spec().resources.is_empty()
            && components.has_background_carrier_pool(slot.role_id())
            && components.stream_dependencies_supported(slot.role_id());
        let mut native = None;
        if slot.dial == Default::default()
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
        }))
    }
}
