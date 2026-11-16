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
        if matches!(input.protocol, OutboundProtocolConfig::VlessReverse) {
            return true;
        }
        if super::super::preconnect::eligible(input) {
            if !native || candidate.is_none() {
                return false;
            }
            let OutboundProtocolConfig::Vless { server, port, .. } = &input.protocol else {
                return false;
            };
            let refs =
                super::super::super::outbound_options(&input.tag, (server, *port), &input.protocol)
                    .expect("owning VLESS projection");
            return ::vless::transport::VlessOutboundLeaf::from_owned_options_refs(
                candidate.and_then(|c| c.source_dir()),
                refs,
                &self.runtime,
            )
            .is_ok_and(|(leaf, _)| {
                leaf.native_preconnect_eligible() && leaf.native_hysteria_eligible()
            });
        }
        if super::super::browser::eligible(input) {
            return native && candidate.is_some();
        }
        if super::super::xhttp::eligible(input) {
            return native && candidate.is_some();
        }
        if super::super::quic::eligible(input) {
            if !native || candidate.is_none() {
                return false;
            }
            let OutboundProtocolConfig::Vless { server, port, .. } = &input.protocol else {
                return false;
            };
            let refs = super::super::super::outbound_options(
                input.tag(),
                (server, *port),
                &input.protocol,
            )
            .expect("owning VLESS projection");
            return ::vless::transport::VlessOutboundLeaf::from_owned_options_refs(
                candidate.and_then(|c| c.source_dir()),
                refs,
                &self.runtime,
            )
            .is_ok_and(|(leaf, _)| leaf.native_hysteria_eligible());
        }
        if super::super::grpc::eligible(input) && !vision(input) {
            return super::super::grpc::selected(input, candidate, native);
        }
        if super::super::owned_input(input) && !vision(input) {
            return true;
        }
        let grpc = grpc_eligible(input);
        if !eligible(input)
            || if grpc {
                !super::super::grpc::selected(input, candidate, native)
            } else {
                let Some(config) = candidate.filter(|_| native) else {
                    return false;
                };
                needs_ip_backend(input) && crate::adapters::node_dns::validate(config).is_err()
            }
        {
            return false;
        }
        let OutboundProtocolConfig::Vless { server, port, .. } = &input.protocol else {
            return false;
        };
        let refs =
            super::super::super::outbound_options(&input.tag, (server, *port), &input.protocol)
                .expect("owning vless projection");
        ::vless::transport::VlessOutboundLeaf::from_owned_options_refs(
            candidate.and_then(|c| c.source_dir()),
            refs,
            &self.runtime,
        )
        .map(|(leaf, _)| leaf.native_mux_eligible() && leaf.native_hysteria_eligible() &&
            (!matches!(&input.protocol,OutboundProtocolConfig::Vless{testpre,..} if *testpre!=0) || leaf.native_preconnect_eligible()))
        .unwrap_or(true)
    }
}

pub(crate) fn needs_ip_backend(input: &OutboundConfig) -> bool {
    let OutboundProtocolConfig::Vless { server, tls, .. } = &input.protocol else {
        return false;
    };
    if zero_core::address::parse_ip_literal(server).is_none() {
        return true;
    }
    let Some(tls) = tls else {
        return false;
    };
    let options = tls.options.to_options();
    match options.ech.source() {
        Ok(Some(zero_traits::EchConfigSource::Dns { server, .. })) => {
            zero_dns::EchDnsEndpoint::parse(server)
                .map(|endpoint| endpoint.literal_peer().is_none())
                .unwrap_or(true)
        }
        Err(_) => true,
        _ => false,
    }
}
