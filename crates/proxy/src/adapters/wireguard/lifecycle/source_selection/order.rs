//! Bound cold WG dependency traversal before aliases or protocol values decode.
use super::*;
use std::collections::BTreeSet;
fn members<'a>(
    config: &'a zero_config::RuntimeConfig,
    outbound: &'a OutboundConfig,
) -> Vec<&'a str> {
    let OutboundProtocolConfig::Wireguard {
        outer_udp_proxy: Some(outer),
        ..
    } = &outbound.protocol
    else {
        return vec![];
    };
    if config.outbounds.iter().any(|node| node.tag() == outer) {
        return vec![outer];
    }
    config
        .outbound_groups
        .iter()
        .find(|group| group.tag == *outer)
        .filter(|group| matches!(group.group, zero_config::OutboundGroupKind::Relay { .. }))
        .map(|group| group.group.members().iter().map(String::as_str).collect())
        .unwrap_or_default()
}
pub(crate) fn acyclic(config: &zero_config::RuntimeConfig, root: &OutboundConfig) -> bool {
    fn visit(
        config: &zero_config::RuntimeConfig,
        node: &OutboundConfig,
        active: &mut BTreeSet<String>,
        done: &mut BTreeSet<String>,
    ) -> bool {
        if done.contains(node.tag()) {
            return true;
        }
        if !active.insert(node.tag.clone()) {
            return false;
        }
        for tag in members(config, node) {
            if let Some(donor) = config
                .outbounds
                .iter()
                .find(|input| input.tag() == tag)
                .filter(|input| matches!(input.protocol, OutboundProtocolConfig::Wireguard { .. }))
            {
                if !visit(config, donor, active, done) {
                    return false;
                }
            }
        }
        active.remove(node.tag());
        done.insert(node.tag.clone());
        true
    }
    visit(config, root, &mut BTreeSet::new(), &mut BTreeSet::new())
}
pub(in crate::adapters::wireguard::lifecycle) fn ordered_devices<'a>(
    config: &'a zero_config::RuntimeConfig,
    resources: &crate::component::ResourcePreparation<'_>,
) -> Result<Vec<&'a OutboundConfig>, EngineError> {
    fn visit<'a>(
        config: &'a zero_config::RuntimeConfig,
        node: &'a OutboundConfig,
        selected: &BTreeSet<&str>,
        active: &mut BTreeSet<&'a str>,
        done: &mut BTreeSet<&'a str>,
        ordered: &mut Vec<&'a OutboundConfig>,
    ) -> Result<(), EngineError> {
        if done.contains(node.tag()) {
            return Ok(());
        }
        if !active.insert(node.tag()) {
            return Err(invalid("cyclic WireGuard cold donor graph"));
        }
        for tag in members(config, node) {
            if let Some(donor) = config
                .outbounds
                .iter()
                .find(|input| input.tag() == tag)
                .filter(|input| matches!(input.protocol, OutboundProtocolConfig::Wireguard { .. }))
            {
                if !selected.contains(donor.tag()) {
                    return Err(invalid("WG donor has no actual selected candidate"));
                }
                visit(config, donor, selected, active, done, ordered)?;
            }
        }
        active.remove(node.tag());
        done.insert(node.tag());
        ordered.push(node);
        Ok(())
    }
    let admission = zero_engine::EndpointAdmission::from_config(config);
    let selected = config
        .outbounds
        .iter()
        .filter(|node| matches!(node.protocol, OutboundProtocolConfig::Wireguard { .. }))
        .filter(|node| {
            admission.device_enabled(node.tag()) && resources.named_component(node.tag()).is_some()
        })
        .map(|node| node.tag())
        .collect::<BTreeSet<_>>();
    let mut ordered = Vec::new();
    let mut done = BTreeSet::new();
    let mut active = BTreeSet::new();
    for node in config
        .outbounds
        .iter()
        .filter(|node| selected.contains(node.tag()))
    {
        visit(
            config,
            node,
            &selected,
            &mut active,
            &mut done,
            &mut ordered,
        )?;
    }
    Ok(ordered)
}
