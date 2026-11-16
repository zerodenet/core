//! One actual adapter pool owns all profiles; callers select their real roles per invocation.
use zero_config::RuntimeConfig;

pub(crate) fn production_selected(candidate: Option<&RuntimeConfig>) -> bool {
    candidate.is_some_and(|config| {
        let nodes = config.outbounds.iter().filter(|node| {
            matches!(
                node.protocol,
                zero_config::OutboundProtocolConfig::Mieru { .. }
            )
        });
        let mut present = false;
        for node in nodes {
            present = true;
            if node
                .protocol
                .endpoint()
                .is_some_and(|(host, _)| zero_core::address::parse_ip_literal(host).is_none())
                && crate::adapters::node_dns::validate(config).is_err()
            {
                return false;
            }
        }
        present
    })
}

#[cfg(test)]
#[path = "production/graph_tests.rs"]
mod graph_tests;
#[cfg(test)]
#[path = "production/tests.rs"]
mod tests;
