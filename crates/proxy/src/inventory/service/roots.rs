//! Cold endpoint facts only; protocol values remain with their actual factory.
use super::*;
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ServiceRoot {
    pub(crate) endpoint: Destination,
    pub(crate) dial: zero_traits::DialPolicy,
}
pub(crate) fn dns_roots(config: &zero_config::RuntimeConfig) -> Vec<ServiceRoot> {
    let mut pending = config
        .runtime
        .dns
        .iter()
        .flat_map(|dns| dns.servers.values())
        .filter_map(|server| server.detour())
        .collect::<Vec<_>>();
    let mut seen = std::collections::BTreeSet::new();
    let mut roots = Vec::new();
    while let Some(tag) = pending.pop() {
        if !seen.insert(tag) {
            continue;
        }
        if let Some(outbound) = config.outbounds.iter().find(|outbound| outbound.tag == tag) {
            if let Some((host, port)) = outbound.protocol.endpoint() {
                let root = ServiceRoot {
                    endpoint: endpoint(host, port),
                    dial: outbound.dial.to_policy(),
                };
                if !roots.contains(&root) {
                    roots.push(root);
                }
            }
        } else if let Some(group) = config.outbound_groups.iter().find(|group| group.tag == tag) {
            pending.extend(group.group.members().iter().map(String::as_str));
        }
    }
    roots
}
