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
        let mut packet_prefix = None;
        let mut packet_carriers = true;
        for name in proxies {
            let (pool, standard) = inspect(config, name, &mut vec![]).unwrap_or((false, false));
            contains_pool |= pool;
            all_standard &= standard;
            match packet_tail(config, name, packet_prefix, &mut vec![]) {
                Some(donor) => packet_prefix = Some(donor),
                None => {
                    packet_carriers = false;
                    packet_prefix = Some(false);
                }
            }
        }
        // Mixed relay preparation keeps every shorter prefix in compatibility
        // when any selected hop lacks an ordinary role. A persistent pool may
        // not reinterpret that one-use prefix as its own native root.
        !contains_pool || (all_standard && packet_carriers)
    })
}

/// A packet-underlay pool needs a real DatagramLoan from its immediately
/// preceding selected role. Stream support or bare UDP ASSOCIATE is insufficient.
/// Choices aggregate every branch; Relay members compose their actual order.
fn packet_tail(
    config: &RuntimeConfig,
    name: &str,
    previous: Option<bool>,
    stack: &mut Vec<String>,
) -> Option<bool> {
    if let Some(node) = config.outbounds.iter().find(|node| node.tag == name) {
        return match &node.protocol {
            OutboundProtocolConfig::Mieru { transport, .. } => {
                if *transport == zero_config::MieruTransport::Udp && previous == Some(false) {
                    None
                } else {
                    Some(true)
                }
            }
            // SOCKS5's current UDP role still owns a bare TCP control and a
            // dynamic BND raw socket. It cannot issue a sealed DatagramLoan.
            _ => Some(false),
        };
    }
    if stack.iter().any(|active| active == name) {
        return None;
    }
    let group = config
        .outbound_groups
        .iter()
        .find(|group| group.tag == name)?;
    stack.push(name.into());
    let result = match &group.group {
        OutboundGroupKind::Relay { proxies } => {
            let mut previous = previous;
            for member in proxies {
                previous = Some(packet_tail(config, member, previous, stack)?);
            }
            previous
        }
        _ => {
            let mut every_tail = true;
            for member in group.group.members() {
                every_tail &= packet_tail(config, member, previous, stack)?;
            }
            Some(every_tail)
        }
    };
    stack.pop();
    result
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
                crate::adapters::vmess::component::native_input(node)
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
