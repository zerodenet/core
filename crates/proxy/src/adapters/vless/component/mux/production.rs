//! Cold ordinary admission; protocol-private ownership is checked by its leaf.
use super::*;
use zero_config::RuntimeConfig;
impl VlessAdapter {
    pub(crate) fn production_selected(
        &self,
        input: &OutboundConfig,
        candidate: Option<&RuntimeConfig>,
        native: bool,
    ) -> bool {
        if super::super::grpc::eligible(input) {
            return super::super::grpc::selected(input, candidate, native);
        }
        if super::super::owned_input(input) {
            return true;
        }
        let grpc = grpc_eligible(input);
        if !eligible(input)
            || if grpc {
                !super::super::grpc::selected(input, candidate, native)
            } else {
                !crate::adapters::mux_prefix::qualified(input, candidate, native)
            }
        {
            return false;
        }
        let OutboundProtocolConfig::Vless {
            server,
            port,
            mux_concurrency,
            xudp_concurrency,
            ..
        } = &input.protocol
        else {
            return false;
        };
        // Do not silently turn ordinary UDP into XUDP, or create a second
        // differently-sized pool under this one physical owner.
        if xudp_concurrency.is_none() || xudp_concurrency != mux_concurrency {
            return false;
        }
        let refs =
            super::super::super::outbound_options(&input.tag, (server, *port), &input.protocol)
                .expect("owning vless projection");
        ::vless::transport::VlessOutboundLeaf::from_options_refs(
            candidate.and_then(|c| c.source_dir()),
            refs,
            &self.runtime,
        )
        .map(|leaf| leaf.native_mux_eligible())
        .unwrap_or(true)
    }
}
