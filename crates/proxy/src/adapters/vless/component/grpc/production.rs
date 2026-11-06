//! Transitional whole-config qualification; excluded legal chains keep old IO.
//! This safety boundary does not complete their original-owner migration.
use super::*;
use std::collections::BTreeSet;
use zero_config::{OutboundGroupKind, RuntimeConfig};
pub(crate) fn has_nodes(config: &RuntimeConfig) -> bool {
    config.outbounds.iter().any(|node| {
        matches!(
            &node.protocol,
            OutboundProtocolConfig::Vless { grpc: Some(_), .. }
        )
    })
}
pub(crate) fn selected(
    input: &OutboundConfig,
    candidate: Option<&RuntimeConfig>,
    native: bool,
) -> bool {
    matches!(
        &input.protocol,
        OutboundProtocolConfig::Vless { grpc: Some(_), .. }
    ) && candidate.is_some_and(|config| whole_selected(config, native))
}
pub(crate) fn whole_selected(config: &RuntimeConfig, native: bool) -> bool {
    if !native {
        return false;
    }
    let nodes = config
        .outbounds
        .iter()
        .filter(|node| {
            matches!(
                &node.protocol,
                OutboundProtocolConfig::Vless { grpc: Some(_), .. }
            )
        })
        .collect::<Vec<_>>();
    !nodes.is_empty()
        && nodes.iter().all(|node| {
            eligible(node)
                && (!ech::needs_ip_backend(node)
                    || zero_dns::native::validate_node_config(config.runtime.dns.as_ref()).is_ok())
                && config.outbound_groups.iter().all(|group| {
                    let OutboundGroupKind::Relay { proxies } = &group.group else {
                        return true;
                    };
                    proxies.iter().all(|name| {
                        reaches(config, name, &node.tag, &mut BTreeSet::new()) == Some(false)
                    })
                })
                && config.outbounds.iter().all(|other| {
                    let OutboundProtocolConfig::Wireguard {
                        outer_udp_proxy: Some(target),
                        ..
                    } = &other.protocol
                    else {
                        return true;
                    };
                    reaches(config, target, &node.tag, &mut BTreeSet::new()) == Some(false)
                })
        })
}
fn reaches(
    config: &RuntimeConfig,
    name: &str,
    wanted: &str,
    active: &mut BTreeSet<String>,
) -> Option<bool> {
    if name == wanted {
        return Some(true);
    }
    if config.outbounds.iter().any(|node| node.tag == name) {
        return Some(false);
    }
    if !active.insert(name.into()) {
        return None;
    }
    let group = config
        .outbound_groups
        .iter()
        .find(|group| group.tag == name)?;
    let mut found = false;
    for child in group.group.members() {
        found |= reaches(config, child, wanted, active)?;
    }
    active.remove(name);
    Some(found)
}

#[cfg(test)]
mod tests;
