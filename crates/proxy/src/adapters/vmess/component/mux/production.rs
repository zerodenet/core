//! Ordinary admission for a root MUX prefix; other compositions keep their owner.
use super::*;
use zero_config::RuntimeConfig;

pub(crate) fn has_mux(input: &OutboundConfig) -> bool {
    matches!(
        input.protocol,
        OutboundProtocolConfig::Vmess {
            mux_concurrency: Some(_),
            ..
        }
    )
}
impl VmessAdapter {
    pub(crate) fn production_selected(
        &self,
        input: &OutboundConfig,
        candidate: Option<&RuntimeConfig>,
        native: bool,
    ) -> bool {
        if !has_mux(input) {
            return super::super::grpc::selected(input, candidate, native);
        }
        let Some(config) = candidate
            .filter(|_| native && eligible(input) && input.dial.to_policy() == Default::default())
        else {
            return false;
        };
        if !crate::adapters::mux_prefix::qualified(input, Some(config), native) {
            return false;
        }
        let OutboundProtocolConfig::Vmess { server, port, .. } = &input.protocol else {
            return false;
        };
        let refs =
            super::super::super::outbound_options(&input.tag, (server, *port), &input.protocol)
                .expect("VMess projection");
        // Invalid selected values are reported by decode. Only actual legacy
        // exposure excludes promotion; no dropped cache is a close credential.
        ::vmess::transport::VmessOutboundLeaf::from_options_refs(
            config.source_dir(),
            refs,
            &self.runtime,
        )
        .map(|leaf| leaf.native_mux_eligible())
        .unwrap_or(true)
    }
}
