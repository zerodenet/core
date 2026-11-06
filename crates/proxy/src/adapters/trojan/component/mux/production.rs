//! Cold ordinary admission; protocol-private ownership is checked by its leaf.
use super::*;
use zero_config::RuntimeConfig;
impl TrojanAdapter {
    pub(crate) fn production_selected(
        &self,
        input: &OutboundConfig,
        candidate: Option<&RuntimeConfig>,
        native: bool,
    ) -> bool {
        if super::super::native_input(input) {
            return true;
        }
        let Some(config) = candidate.filter(|_| native && input.dial.is_default()) else {
            return false;
        };
        if !eligible(input)
            || used_by_relay(config, &input.tag)
                && !crate::adapters::mux_prefix::qualified(input, candidate, native)
        {
            return false;
        }
        let OutboundProtocolConfig::Trojan { server, port, .. } = &input.protocol else {
            return false;
        };
        if zero_core::address::parse_ip_literal(server).is_none()
            && zero_dns::native::validate_node_config(config.runtime.dns.as_ref()).is_err()
        {
            return false;
        }
        let refs =
            super::super::super::outbound_options(&input.tag, (server, *port), &input.protocol)
                .expect("owning trojan projection");
        ::trojan::transport::TrojanOutboundLeaf::from_options_refs(
            candidate.and_then(|c| c.source_dir()),
            refs,
            self.mux_pool.clone(),
        )
        .map(|leaf| leaf.native_mux_eligible())
        .unwrap_or(true)
    }
}

// Inspect every Relay branch, including selectors/groups that are not selected
// now. A root-only node uses the same actual MUX owner for TCP and UDP; a Relay
// use must already have its complete standard shorter-prefix provider.
fn used_by_relay(config: &RuntimeConfig, tag: &str) -> bool {
    fn contains(config: &RuntimeConfig, name: &str, tag: &str, depth: usize) -> bool {
        name == tag
            || depth <= config.outbound_groups.len()
                && config
                    .outbound_groups
                    .iter()
                    .find(|group| group.tag == name)
                    .is_some_and(|group| {
                        group
                            .group
                            .members()
                            .iter()
                            .any(|child| contains(config, child, tag, depth + 1))
                    })
    }
    config.outbound_groups.iter().any(|group| {
        matches!(&group.group, zero_config::OutboundGroupKind::Relay { proxies }
            if proxies.iter().any(|name| contains(config, name, tag, 0)))
    })
}
