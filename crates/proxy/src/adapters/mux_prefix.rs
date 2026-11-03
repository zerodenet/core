//! Shared cold qualification of an actual root MUX used by a gRPC relay.
use zero_config::{OutboundConfig, RuntimeConfig};
#[cfg(feature = "vmess")]
use zero_config::{OutboundGroupKind, OutboundProtocolConfig};
#[cfg(feature = "vmess")]
fn contains(config: &RuntimeConfig, name: &str, tag: &str, depth: usize) -> bool {
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
