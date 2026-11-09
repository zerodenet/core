//! Cold endpoint facts only; protocol values remain with their actual factory.
use super::*;
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ServiceRoot {
    pub(crate) endpoint: Destination,
    pub(crate) datagram: bool,
    /// Only borrowed virtual IO; this declaration supplies no native dial.
    pub(crate) logical: bool,
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
        let mut device = false;
        let mut dials = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        while let Some(tag) = pending.pop() {
            if !seen.insert(tag) {
                continue;
            }
            if let Some(outbound) = config.outbounds.iter().find(|outbound| outbound.tag == tag) {
                let dial = outbound.dial.to_policy();
                if !dials.contains(&dial) {
                    dials.push(dial.clone());
                }
                device |= matches!(
                    &outbound.protocol,
                    zero_config::OutboundProtocolConfig::Wireguard { .. }
                ) && dial != Default::default();
                let endpoints = if outbound.protocol.runtime_kind()
                    == zero_config::OutboundRuntimeKind::Direct
                {
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
                        datagram: false,
                        logical: false,
                    };
                    if !roots.contains(&root) {
                        roots.push(root.clone());
                    }
                    if outbound.protocol.runtime_kind() == zero_config::OutboundRuntimeKind::Direct
                        && matches!(server, zero_config::DnsServerConfig::Udp { .. })
                    {
                        let datagram = ServiceRoot {
                            datagram: true,
                            ..root
                        };
                        if !roots.contains(&datagram) {
                            roots.push(datagram);
                        }
                    }
                }
            } else if let Some(group) = config.outbound_groups.iter().find(|group| group.tag == tag)
            {
                pending.extend(group.group.members().iter().map(String::as_str));
            }
        }
        if device {
            // These are pure logical query owners. The actual Packet role still
            // validates its installed provider/Scope and owns the underlay dial.
            // Include each reachable final dial so existing Relay/Fallback paths
            // keep their own final binding, never the first device's host bind.
            for dial in dials {
                for address in server.endpoint_addresses().unwrap_or_default() {
                    let Some(port) = server.port() else { continue };
                    for datagram in [false, true] {
                        if datagram && !matches!(server, zero_config::DnsServerConfig::Udp { .. }) {
                            continue;
                        }
                        let root = ServiceRoot {
                            endpoint: endpoint(&address.to_string(), port),
                            dial: dial.clone(),
                            datagram,
                            logical: true,
                        };
                        if !roots.contains(&root) {
                            roots.push(root);
                        }
                    }
                }
            }
        }
    }
    roots
}
