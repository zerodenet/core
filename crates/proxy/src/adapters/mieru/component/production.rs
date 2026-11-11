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
        let mut prefix = None;
        let mut packet_carriers = true;
        for name in proxies {
            let (pool, standard) = inspect(config, name, &mut vec![]).unwrap_or((false, false));
            contains_pool |= pool;
            all_standard &= standard;
            match packet_tail(config, name, prefix, &mut vec![]) {
                Some(tail) => prefix = Some(tail),
                None => {
                    packet_carriers = false;
                    prefix = Some(Tail::UNMIGRATED);
                }
            }
        }
        // Mixed relay preparation keeps every shorter prefix in compatibility
        // when any selected hop lacks an ordinary role. A persistent pool may
        // not reinterpret that one-use prefix as its own native root.
        // The last hop consumes business traffic, it need not provide a packet
        // carrier to another pool. In particular, a non-root SOCKS5 has never
        // supported UDP relay: preserving that rejection must not leave the
        // whole shared pool on the compatibility executor. Only an
        // actual UDP-underlay receiver below requires its donor's DatagramLoan.
        !contains_pool
            || (all_standard && packet_carriers && prefix.is_some_and(|tail| tail.business))
    })
}

#[derive(Clone, Copy)]
struct Tail {
    donor: bool,
    business: bool,
}
impl Tail {
    const UNMIGRATED: Self = Self {
        donor: false,
        business: false,
    };
}

/// A packet-underlay pool needs a real DatagramLoan from its immediately
/// preceding selected role. Stream support or bare UDP ASSOCIATE is insufficient.
/// Choices aggregate every branch; Relay members compose their actual order.
fn packet_tail(
    config: &RuntimeConfig,
    name: &str,
    previous: Option<Tail>,
    stack: &mut Vec<String>,
) -> Option<Tail> {
    if let Some(node) = config.outbounds.iter().find(|node| node.tag == name) {
        return match &node.protocol {
            OutboundProtocolConfig::Mieru { transport, .. } => {
                if *transport == zero_config::MieruTransport::Udp
                    && previous.is_some_and(|tail| !tail.donor)
                {
                    None
                } else {
                    Some(Tail {
                        donor: true,
                        business: true,
                    })
                }
            }
            // The root SOCKS association now transfers the real TCP+negotiated
            // UDP originals in one sealed LoanOwner. A non-root SOCKS packet
            // capability never existed and remains unqualified here.
            OutboundProtocolConfig::Socks5 { .. } => Some(Tail {
                donor: previous.is_none(),
                business: true,
            }),
            #[cfg(feature = "wireguard")]
            OutboundProtocolConfig::Wireguard {
                inbound_tag: Some(_),
                outer_udp_proxy: None,
                ..
            } => previous.is_none().then_some(Tail {
                donor: true,
                business: true,
            }),
            _ => Some(Tail::UNMIGRATED),
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
            let mut every_tail = Tail {
                donor: true,
                business: true,
            };
            for member in group.group.members() {
                let tail = packet_tail(config, member, previous, stack)?;
                every_tail.donor &= tail.donor;
                every_tail.business &= tail.business;
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
            #[cfg(feature = "wireguard")]
            OutboundProtocolConfig::Wireguard {
                inbound_tag: Some(_),
                outer_udp_proxy: None,
                ..
            } => true,
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

#[cfg(test)]
#[path = "production/graph_tests.rs"]
mod graph_tests;
