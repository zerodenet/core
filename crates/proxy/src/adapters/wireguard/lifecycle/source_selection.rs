//! Select only existing whole-owning factories before cold dependency work.
//! Every selected Source requires its complete original-owning dependency graph;
//! absence never authorizes a private device constructor or a partial recipe.
use super::*;
mod order;
pub(crate) use order::acyclic;
pub(super) use order::ordered_devices;
pub(crate) fn outer_members_selected(
    outbound: &OutboundConfig,
    config: &zero_config::RuntimeConfig,
    mut selected: impl FnMut(&OutboundConfig) -> bool,
) -> bool {
    let OutboundProtocolConfig::Wireguard {
        outer_udp_proxy: Some(tag),
        ..
    } = &outbound.protocol
    else {
        return true;
    };
    if let Some(member) = config.outbounds.iter().find(|member| member.tag == *tag) {
        return selected(member);
    }
    let Some(group) = config
        .outbound_groups
        .iter()
        .find(|group| group.tag == *tag)
    else {
        return false;
    };
    let zero_config::OutboundGroupKind::Relay { proxies } = &group.group else {
        return false;
    };
    !proxies.is_empty()
        && proxies.iter().all(|tag| {
            config
                .outbounds
                .iter()
                .find(|member| member.tag == *tag)
                .is_some_and(&mut selected)
        })
}

#[cfg(test)]
mod tests;
