//! One adapter pool cannot be native and compatibility-owned at the same time.
use zero_config::{OutboundGroupKind, OutboundProtocolConfig, RuntimeConfig};

pub(crate) fn production_selected(candidate: Option<&RuntimeConfig>) -> bool {
    let Some(config) = candidate else {
        return false;
    };
    config.outbound_groups.iter().all(|group| {
        let OutboundGroupKind::Relay { proxies } = &group.group else {
            return true;
        };
        let mut contains_pool = false;
        let mut all_standard = true;
        for name in proxies {
            let Some((pool, standard)) = inspect(config, name, &mut vec![]) else {
                return false;
            };
            contains_pool |= pool;
            all_standard &= standard;
        }
        // Mixed relay preparation keeps every shorter prefix in compatibility
        // when any selected hop lacks an ordinary role. A persistent pool may
        // not reinterpret that one-use prefix as its own native root.
        !contains_pool || all_standard
    })
}

fn inspect(config: &RuntimeConfig, name: &str, stack: &mut Vec<String>) -> Option<(bool, bool)> {
    if let Some(node) = config.outbounds.iter().find(|node| node.tag == name) {
        let pooled = matches!(node.protocol, OutboundProtocolConfig::Mieru { .. });
        let standard = match &node.protocol {
            OutboundProtocolConfig::Mieru { .. } | OutboundProtocolConfig::Socks5 { .. } => true,
            #[cfg(feature = "vless")]
            OutboundProtocolConfig::Vless { .. } => {
                crate::adapters::vless::component::native_input(node)
            }
            #[cfg(feature = "vmess")]
            OutboundProtocolConfig::Vmess { .. } => {
                crate::adapters::vmess::component::grpc::selected(node, Some(config), true)
            }
            #[cfg(feature = "trojan")]
            OutboundProtocolConfig::Trojan { .. } => {
                crate::adapters::trojan::component::native_input(node)
            }
            #[cfg(feature = "shadowsocks")]
            OutboundProtocolConfig::Shadowsocks { .. } => {
                crate::adapters::shadowsocks::component::native_input(node)
            }
            // Other existing styles need their persistent original provider
            // migration before this shared pool can borrow that whole graph.
            _ => false,
        };
        return Some((pooled, standard));
    }
    if stack.iter().any(|active| active == name) {
        return None;
    }
    let group = config
        .outbound_groups
        .iter()
        .find(|group| group.tag == name)?;
    stack.push(name.into());
    let mut pooled = false;
    let mut standard = true;
    for name in group.group.members() {
        let (child_pool, child_standard) = inspect(config, name, stack)?;
        pooled |= child_pool;
        standard &= child_standard;
    }
    stack.pop();
    Some((pooled, standard))
}

#[cfg(test)]
#[path = "production/tests.rs"]
mod tests;
