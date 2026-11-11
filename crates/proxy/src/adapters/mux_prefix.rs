//! Shared cold qualification of an actual root MUX used by a gRPC relay.
#[cfg(feature = "vmess")]
use zero_config::OutboundProtocolConfig;
use zero_config::{OutboundConfig, OutboundGroupKind, RuntimeConfig};
pub(crate) fn contains(config: &RuntimeConfig, name: &str, tag: &str, depth: usize) -> bool {
    name == tag
        || (depth <= config.outbound_groups.len()
            && config
                .outbound_groups
                .iter()
                .find(|group| group.tag == name)
                .is_some_and(|group| {
                    group
                        .group
                        .members()
                        .iter()
                        .any(|child| contains(config, child, tag, depth + 1))
                }))
}
/// All relay aliases matter, including currently unselected choices. A
/// stateless role used by an unmigrated persistent consumer must still retain
/// its existing inline executor until that consumer can transfer the prefix.
pub(crate) fn used_by_relay(config: &RuntimeConfig, tag: &str) -> bool {
    config.outbound_groups.iter().any(|group| {
        matches!(&group.group, OutboundGroupKind::Relay { proxies }
            if proxies.iter().any(|name| contains(config, name, tag, 0)))
    })
}
/// Device consumers still need their cold packet-format projection. Qualified
/// Sources consume only those facts through their actual host; the executable
/// legacy outer-UDP edge remains TEMP-WG-OUTER-UDP-01. This graph fact grants
/// neither a legacy fallback nor evidence of native closure.
pub(crate) fn used_by_outer_udp(config: &RuntimeConfig, tag: &str) -> bool {
    config.outbounds.iter().any(|node| {
        matches!(&node.protocol, zero_config::OutboundProtocolConfig::Wireguard {
            outer_udp_proxy: Some(target), ..
        } if contains(config, target, tag, 0))
    })
}
pub(crate) fn qualified(
    input: &OutboundConfig,
    candidate: Option<&RuntimeConfig>,
    native: bool,
) -> bool {
    #[cfg(not(feature = "vmess"))]
    {
        let _ = (input, candidate, native);
        false
    }
    #[cfg(feature = "vmess")]
    {
        let Some(config) = candidate.filter(|_| native && input.dial.is_default()) else {
            return false;
        };
        let mut qualified = false;
        for group in &config.outbound_groups {
            let OutboundGroupKind::Relay { proxies } = &group.group else {
                continue;
            };
            if !proxies
                .iter()
                .any(|name| contains(config, name, &input.tag, 0))
            {
                continue;
            }
            // One pool currently has one active upstream epoch. Do not promote
            // nodes used behind other carriers or ambiguous nested relays.
            if proxies.len() < 2
                || proxies.first() != Some(&input.tag)
                || proxies
                    .iter()
                    .any(|name| config.outbound_groups.iter().any(|g| &g.tag == name))
                || proxies.iter().skip(1).any(|name| name == &input.tag)
            {
                return false;
            }
            // Intermediate stateless carriers must already have an executable
            // ordinary role. Resourceful/unmigrated prefixes keep compatibility.
            if proxies[1..proxies.len() - 1].iter().any(|name| {
                let Some(node) = config.outbounds.iter().find(|node| &node.tag == name) else {
                    return true;
                };
                if node.dial.to_policy() != Default::default() {
                    return true;
                }
                match &node.protocol {
                    OutboundProtocolConfig::Socks5 { .. } => false,
                    OutboundProtocolConfig::Vmess { .. } => {
                        !super::vmess::component::native_input(node)
                    }
                    _ => true,
                }
            }) {
                return false;
            }
            let Some(last) = config
                .outbounds
                .iter()
                .find(|node| Some(&node.tag) == proxies.last())
            else {
                return false;
            };
            if last.dial.to_policy() != Default::default()
                || !matches!(
                    last.protocol,
                    OutboundProtocolConfig::Vmess {
                        grpc: Some(_),
                        mux_concurrency: None,
                        ..
                    }
                )
                || !super::vmess::component::grpc::selected(last, Some(config), native)
            {
                return false;
            }
            qualified = true;
        }
        if !qualified {
            return false;
        }
        true
    }
}

#[cfg(test)]
#[path = "mux_prefix/tests.rs"]
mod tests;
