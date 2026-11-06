//! The existing packet-capable DNS detour lends its standard datagram role.
use super::*;
use zero_platform_tokio::contract::component::RoleKind;

pub(crate) struct ServiceDatagram {
    pub(crate) slot: Arc<ComponentSlot>,
    pub(crate) tag: Option<String>,
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
        if !roles.contains(&RoleKind::Packet) || !roles.contains(&RoleKind::Datagram) {
            return Ok(None);
        }
        Ok(Some(ServiceDatagram {
            slot,
            tag: Some(outbound.tag.clone()),
        }))
    }
}
