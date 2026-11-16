//! Canonical Source paths use only their actual build root and Stream donors.
//! Trailing codecs retain the exact cold-pass family without TCP preparation.
use super::*;
use crate::{
    component::stream_bridge::datagram::packet_path::RolePacketPath,
    protocol_registry::{ColdPacketPathProjection, OutboundAdapterContext},
};

/// Preserve the existing named-leaf/nonempty flat-Relay grammar. Resolving a
/// concrete path here never selects a dynamic group or creates an executor.
pub(super) fn indices(config: &RuntimeConfig, tag: &str) -> Result<Vec<usize>, EngineError> {
    let index = |name: &str| {
        config
            .outbounds
            .iter()
            .position(|input| input.tag() == name)
            .ok_or_else(|| {
                invalid(format!(
                    "outer Packet member `{name}` is not a concrete outbound"
                ))
            })
    };
    if let Some(index) = config.outbounds.iter().position(|input| input.tag() == tag) {
        return Ok(vec![index]);
    }
    let group = config
        .outbound_groups
        .iter()
        .find(|group| group.tag == tag)
        .ok_or_else(|| invalid(format!("outer Packet path `{tag}` is absent")))?;
    let zero_config::OutboundGroupKind::Relay { proxies } = &group.group else {
        return Err(invalid(
            "outer Packet path requires its existing flat Relay",
        ));
    };
    if proxies.is_empty() {
        return Err(invalid("outer Packet Relay has no actual members"));
    }
    proxies.iter().map(|name| index(name)).collect()
}

impl ProtocolInventory {
    pub(super) fn prepare_role_packet_path(
        &self,
        config: &RuntimeConfig,
        indices: &[usize],
        plan: &crate::component::catalog::ComponentPlan,
        network_generation: zero_core::component::NetworkGeneration,
        sources: &[Option<ColdPacketPathProjection>],
    ) -> Result<RolePacketPath, EngineError> {
        if indices.is_empty() || sources.len() != indices.len() {
            return Err(invalid(
                "Source packet path does not match its actual members",
            ));
        }
        let root = sources
            .iter()
            .rposition(|source| {
                source
                    .as_ref()
                    .is_none_or(|source| source.formatter.is_none())
            })
            .unwrap_or(0);
        let projection = sources[root]
            .as_ref()
            .ok_or_else(|| invalid("Source root has no actual Packet facts"))?;
        let tag = projection
            .descriptor
            .tag
            .as_deref()
            .ok_or_else(|| invalid("Source root has no configured owner tag"))?;
        if config
            .outbounds
            .get(indices[root])
            .is_none_or(|input| input.tag() != tag)
        {
            return Err(invalid(
                "Source root facts differ from the actual selected member",
            ));
        }
        let slot = plan
            .configured(tag)
            .ok_or_else(|| invalid("Source root has no actual standard component"))?;
        let prefix = if root == 0 {
            None
        } else {
            if !projection.associated {
                return Err(invalid(
                    "Source receiver has no existing Associated grammar",
                ));
            }
            let donors = indices[..root].iter().map(|index| {
                let leaf = zero_engine::ResolvedLeafOutbound::Proxy {
                    identity: zero_engine::OutboundIdentity::from_config_index(*index),
                };
                self.native_tcp_candidate(
                    OutboundAdapterContext::new(config),
                    &leaf,
                    Some(plan),
                    zero_engine::RouteMode::Flow,
                )
            });
            Some(
                Self::prepare_native_tcp_relay_chain(donors)
                    .ok_or_else(|| invalid("Source prefix lacks its actual standard Stream roles"))?
                    .map_err(|failure| failure.error)?,
            )
        };
        let mut path =
            RolePacketPath::prepare(&slot, projection, prefix, plan, network_generation)?;
        for (index, source) in sources.iter().enumerate().skip(root + 1) {
            let source = source
                .as_ref()
                .ok_or_else(|| invalid("Source tail lost its actual cold facts"))?;
            if source.descriptor.tag.as_deref() != Some(config.outbounds[indices[index]].tag()) {
                return Err(invalid(
                    "Source tail facts differ from their selected owner",
                ));
            }
            let formatter = source
                .formatter
                .clone()
                .ok_or_else(|| invalid("Source tail lost its actual cold formatter"))?;
            path.append_formatter(formatter);
        }
        Ok(path)
    }
}
