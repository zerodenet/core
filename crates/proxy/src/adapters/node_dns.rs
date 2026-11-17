//! Node queries borrow the same real standard detour providers as Default DNS.
//! Validate bootstrap independence here, at the configuration adapter boundary.
use std::{collections::BTreeSet, io};
use zero_config::{OutboundProtocolConfig as P, RuntimeConfig};

pub(crate) fn validate(config: &RuntimeConfig) -> io::Result<()> {
    zero_dns::native::validate_node_config_with_detours(config.runtime.dns.as_ref(), |tag| {
        validate_detour(config, tag)
    })
}
pub(crate) fn validate_detour(config: &RuntimeConfig, tag: &str) -> io::Result<()> {
    bootstrap_path(config, tag, &mut BTreeSet::new())
}
fn unsupported(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, message)
}
fn bootstrap_path(
    config: &RuntimeConfig,
    tag: &str,
    active: &mut BTreeSet<String>,
) -> io::Result<()> {
    if !active.insert(tag.into()) {
        return Err(unsupported("Node DNS detour contains a dependency cycle"));
    }
    let result = (|| {
        if let Some(node) = config.outbounds.iter().find(|node| node.tag == tag) {
            if node
                .protocol
                .endpoint()
                .is_some_and(|(host, _)| zero_core::address::parse_ip_literal(host).is_none())
            {
                // Resolving this carrier through the same selected Node source
                // would wait on the query that is currently opening it. Keep
                // the existing explicit bootstrap requirement, not a timed-out
                // recursive query or a direct-network bypass.
                return Err(unsupported(
                    "Node DNS detour itself requires Node resolution",
                ));
            }
            let tls = match &node.protocol {
                P::Vless { tls, .. } | P::Vmess { tls, .. } => tls.as_ref(),
                _ => None,
            };
            if let Some(tls) = tls {
                let options = tls.options.to_options();
                if let Some(zero_traits::EchConfigSource::Dns { server, .. }) =
                    options.ech.source().map_err(|source| {
                        io::Error::new(io::ErrorKind::InvalidInput, source.to_string())
                    })?
                {
                    zero_dns::EchDnsEndpoint::parse(server)?;
                    return Err(unsupported(
                        "Node DNS detour ECH depends on the same Node owner",
                    ));
                }
            }
            // Device bootstrap and its retained outer dependency graph have a
            // separate provider; do not turn TEMP-WG into anonymous Node I/O.
            if matches!(node.protocol, P::Wireguard { .. } | P::Block) {
                return Err(unsupported(
                    "Node DNS detour lacks an independent carrier bootstrap",
                ));
            }
            Ok(())
        } else if let Some(group) = config.outbound_groups.iter().find(|group| group.tag == tag) {
            for member in group.group.members() {
                bootstrap_path(config, member, active)?;
            }
            Ok(())
        } else if tag == "direct" {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("undefined Node DNS detour `{tag}`"),
            ))
        }
    })();
    active.remove(tag);
    result
}

#[cfg(test)]
mod tests;
