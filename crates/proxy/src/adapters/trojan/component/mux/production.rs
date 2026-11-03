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
        if !eligible(input) || !crate::adapters::mux_prefix::qualified(input, candidate, native) {
            return false;
        }
        let OutboundProtocolConfig::Trojan { server, port, .. } = &input.protocol else {
            return false;
        };
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
