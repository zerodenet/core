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
        if crate::adapters::browser::eligible(input) {
            return native && candidate.is_some();
        }
        if super::super::native_input(input) {
            return true;
        }
        let Some(config) = candidate.filter(|_| native) else {
            return false;
        };
        if !eligible(input) {
            return false;
        }
        let OutboundProtocolConfig::Trojan { server, port, .. } = &input.protocol else {
            return false;
        };
        if zero_core::address::parse_ip_literal(server).is_none()
            && crate::adapters::node_dns::validate(config).is_err()
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
