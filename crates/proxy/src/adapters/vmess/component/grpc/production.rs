//! One Node cannot expose compatibility TCP beside its new actual Stream owner.
use super::*;
use zero_config::{OutboundGroupKind, RuntimeConfig};
pub(crate) fn streams_selected(
    input: &OutboundConfig,
    candidate: Option<&RuntimeConfig>,
    native: bool,
) -> bool {
    if !matches!(
        input.protocol,
        OutboundProtocolConfig::Vmess { grpc: Some(_), .. }
    ) || !selected(input, candidate, native)
    {
        return false;
    }
    let Some(config) = candidate else {
        return false;
    };
    // Inspect every configured branch, including currently unselected aliases.
    // Resourceful/mixed TCP relay callers still use their original owner. Until
    // they can transfer their real prefix, this whole Node keeps its old TCP
    // surface; it never exposes the new Stream beside that compatibility SPI.
    config.outbound_groups.iter().all(|group| {
        let OutboundGroupKind::Relay { proxies } = &group.group else {
            return true;
        };
        proxies.iter().all(|name| {
            reaches(
                config,
                name,
                input.tag(),
                &mut std::collections::BTreeSet::new(),
            ) == Some(false)
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
