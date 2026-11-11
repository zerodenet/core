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
        if matches!(
            &input.protocol,
            OutboundProtocolConfig::Vmess { grpc: Some(_), .. }
        ) && !super::super::grpc::whole_streams_selected(config, native)
        {
            return false;
        }
        if needs_ip_backend(input)
            && zero_dns::native::validate_node_config(config.runtime.dns.as_ref()).is_err()
        {
            return false;
        }
        if crate::adapters::mux_prefix::used_by_relay(config, &input.tag)
            && !crate::adapters::mux_prefix::qualified(input, Some(config), native)
        {
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

pub(crate) fn needs_ip_backend(input: &OutboundConfig) -> bool {
    let OutboundProtocolConfig::Vmess { server, tls, .. } = &input.protocol else {
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
