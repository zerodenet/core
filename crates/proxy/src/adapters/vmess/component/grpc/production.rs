//! One Node cannot expose compatibility TCP beside its new actual Stream owner.
use super::*;
use zero_config::RuntimeConfig;
pub(crate) fn streams_selected(
    input: &OutboundConfig,
    candidate: Option<&RuntimeConfig>,
    native: bool,
) -> bool {
    matches!(
        input.protocol,
        OutboundProtocolConfig::Vmess { grpc: Some(_), .. }
    ) && candidate.is_some_and(|config| whole_streams_selected(config, native))
}
pub(crate) fn whole_streams_selected(config: &RuntimeConfig, native: bool) -> bool {
    native
        && has_grpc_nodes(config)
        && config
            .outbounds
            .iter()
            .filter(|node| {
                matches!(
                    &node.protocol,
                    OutboundProtocolConfig::Vmess { grpc: Some(_), .. }
                )
            })
            .all(|node| {
                if super::super::mux::has_mux(node) {
                    super::super::mux::eligible(node)
                        && (!super::super::mux::needs_ip_backend(node)
                            || crate::adapters::node_dns::validate(config).is_ok())
                } else {
                    selected(node, Some(config), native)
                }
            })
}

#[cfg(test)]
mod tests;

pub(crate) fn has_grpc_nodes(config: &RuntimeConfig) -> bool {
    config.outbounds.iter().any(|node| {
        matches!(
            node.protocol,
            OutboundProtocolConfig::Vmess { grpc: Some(_), .. }
        )
    })
}
