//! One Node cannot expose compatibility TCP beside its new actual Stream owner.
use super::*;
use zero_config::{OutboundGroupKind, RuntimeConfig};
pub(crate) fn streams_selected(
    input: &OutboundConfig,
    candidate: Option<&RuntimeConfig>,
    native: bool,
) -> bool {
    matches!(
        input.protocol,
        OutboundProtocolConfig::Vmess { grpc: Some(_), .. }
    ) && candidate.is_some_and(|config| whole_streams_selected(config, native))
}
pub(crate) fn whole_streams_selected(config: &RuntimeConfig, native: bool) -> bool {
    let nodes = config
        .outbounds
        .iter()
        .filter(|node| {
            matches!(
                node.protocol,
                OutboundProtocolConfig::Vmess { grpc: Some(_), .. }
            )
        })
        .collect::<Vec<_>>();
    !nodes.is_empty()
        && nodes.into_iter().all(|node| {
            selected(node, Some(config), native)
                && config.outbound_groups.iter().all(|group| {
                    let OutboundGroupKind::Relay { proxies } = &group.group else {
                        return true;
                    };
                    proxies.iter().all(|name| {
                        reaches(
                            config,
                            name,
                            node.tag(),
                            &mut std::collections::BTreeSet::new(),
                        ) == Some(false)
                    })
                })
        })
}

fn reaches(
    config: &RuntimeConfig,
    name: &str,
    wanted: &str,
    active: &mut std::collections::BTreeSet<String>,
) -> Option<bool> {
    if name == wanted {
        return Some(true);
    }
    if config
        .outbounds
        .iter()
        .any(|outbound| outbound.tag() == name)
    {
        return Some(false);
    }
    if !active.insert(name.into()) {
        return None;
    }
    let group = config
        .outbound_groups
        .iter()
        .find(|group| group.tag() == name)?;
    let mut found = false;
    for child in group.group.members() {
        found |= reaches(config, child, wanted, active)?;
    }
    active.remove(name);
    Some(found)
}
#[cfg(test)]
mod tests;
