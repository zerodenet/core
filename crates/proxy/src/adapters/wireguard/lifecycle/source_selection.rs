//! Select only existing whole-owning factories before cold dependency work.
//! Every selected Source requires its complete original-owning dependency graph;
//! absence never authorizes a private device constructor or a partial recipe.
use super::*;
mod order;
pub(crate) use order::acyclic;
pub(super) use order::ordered_devices;

pub(crate) fn native_candidate_selected(
    outbound: &OutboundConfig,
    config: &zero_config::RuntimeConfig,
    other_factory_selected: impl Fn(&OutboundConfig) -> bool,
) -> bool {
    let admission = zero_engine::EndpointAdmission::from_config(config);
    let mut accepted = std::collections::BTreeSet::new();
    fn visit(
        outbound: &OutboundConfig,
        config: &zero_config::RuntimeConfig,
        admission: &zero_engine::EndpointAdmission,
        accepted: &mut std::collections::BTreeSet<String>,
        other_factory_selected: &impl Fn(&OutboundConfig) -> bool,
    ) -> bool {
        if accepted.contains(outbound.tag()) {
            return true;
        }
        // The packet-source grammar rejects a cycle before inspecting any
        // member's factory. A WG donor must pass this same whole-path gate.
        let selected = admission.device_enabled(outbound.tag())
            && crate::adapters::mux_prefix::packet_source_selected(config, outbound)
            && outer_members_selected(outbound, config, |member| {
                if matches!(member.protocol, OutboundProtocolConfig::Wireguard { .. }) {
                    visit(member, config, admission, accepted, other_factory_selected)
                } else {
                    other_factory_selected(member)
                }
            });
        if selected {
            accepted.insert(outbound.tag.clone());
        }
        selected
    }
    visit(
        outbound,
        config,
        &admission,
        &mut accepted,
        &other_factory_selected,
    )
}

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
