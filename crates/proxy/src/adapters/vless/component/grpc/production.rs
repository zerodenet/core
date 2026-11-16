//! The actual registry moves its profiles together. Caller routing selects
//! standard roles per invocation; an unrelated relay cannot select a legacy pool.
use super::*;
use zero_config::RuntimeConfig;
pub(crate) fn has_nodes(config: &RuntimeConfig) -> bool {
    config.outbounds.iter().any(|node| {
        matches!(
            &node.protocol,
            OutboundProtocolConfig::Vless { grpc: Some(_), .. }
        )
    })
}
pub(crate) fn selected(
    input: &OutboundConfig,
    candidate: Option<&RuntimeConfig>,
    native: bool,
) -> bool {
    matches!(
        &input.protocol,
        OutboundProtocolConfig::Vless { grpc: Some(_), .. }
    ) && candidate.is_some_and(|config| whole_selected(config, native))
}
pub(crate) fn whole_selected(config: &RuntimeConfig, native: bool) -> bool {
    native
        && has_nodes(config)
        && config
            .outbounds
            .iter()
            .filter(|node| {
                matches!(
                    &node.protocol,
                    OutboundProtocolConfig::Vless { grpc: Some(_), .. }
                )
            })
            .all(|node| {
                (eligible(node)
                    || super::super::mux::grpc_eligible(node)
                    || super::super::preconnect::eligible(node))
                    && (!ech::needs_ip_backend(node)
                        || crate::adapters::node_dns::validate(config).is_ok())
            })
}

#[cfg(test)]
mod tests;
