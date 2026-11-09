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
        if super::super::quic::eligible(input) {
            return native && candidate.is_some();
        }
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
                let Some(config) = candidate.filter(|_| native && input.dial.is_default()) else {
                    return false;
                };
                needs_ip_backend(input)
                    && zero_dns::native::validate_node_config(config.runtime.dns.as_ref()).is_err()
                    || used_by_relay(config, &input.tag)
                        && !crate::adapters::mux_prefix::qualified(input, candidate, native)
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

fn needs_ip_backend(input: &OutboundConfig) -> bool {
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
