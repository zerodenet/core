//! Cold endpoint facts only; protocol values remain with their actual factory.
use super::*;
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ServiceRoot {
    pub(crate) endpoint: Destination,
    pub(crate) dial: zero_traits::DialPolicy,
}
pub(crate) fn dns_roots(config: &zero_config::RuntimeConfig) -> Vec<ServiceRoot> {
    let mut roots = Vec::new();
    for server in config
        .runtime
        .dns
        .iter()
        .flat_map(|dns| dns.servers.values())
    {
        let Some(detour) = server.detour() else {
            continue;
        };
        let mut pending = vec![detour];
        let mut seen = std::collections::BTreeSet::new();
        while let Some(tag) = pending.pop() {
            if !seen.insert(tag) {
                continue;
            }
            if let Some(outbound) = config.outbounds.iter().find(|outbound| outbound.tag == tag) {
                let endpoints =
                    if outbound.protocol.runtime_kind() == zero_config::OutboundRuntimeKind::Direct {
                        server
                            .port()
                            .into_iter()
                            .flat_map(|port| {
                                server
                                    .endpoint_addresses()
                                    .unwrap_or_default()
                                    .into_iter()
                                    .map(move |address| endpoint(&address.to_string(), port))
                            })
                            .collect::<Vec<_>>()
                    } else {
                        outbound
                            .protocol
                            .endpoint()
                            .into_iter()
                            .map(|(host, port)| endpoint(host, port))
                            .collect()
                    };
                for endpoint in endpoints {
                    let root = ServiceRoot {
                        endpoint,
                        dial: outbound.dial.to_policy(),
                    };
                    if !roots.contains(&root) {
                        roots.push(root);
                    }
                }
            } else if let Some(group) = config.outbound_groups.iter().find(|group| group.tag == tag)
            {
                pending.extend(group.group.members().iter().map(String::as_str));
            }
        }
    }
    roots
}
