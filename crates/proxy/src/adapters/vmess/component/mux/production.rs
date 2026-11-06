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
        if needs_ip_backend(input)
            && zero_dns::native::validate_node_config(config.runtime.dns.as_ref()).is_err()
        {
            return false;
        }
        if used_by_relay(config, &input.tag)
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

fn needs_ip_backend(input: &OutboundConfig) -> bool {
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
