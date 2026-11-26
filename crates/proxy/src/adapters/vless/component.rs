//! Cold ordinary Stream/Datagram selection into actual protocol-owned factories.
//! H2/HTTPUpgrade retain their inline connection inside the same original loan.
pub(crate) mod base;
mod browser;
pub(crate) mod grpc;
mod material;
pub(crate) mod mux;
mod preconnect;
pub(crate) mod quic;
mod reverse;
pub(crate) mod xhttp;
use zero_config::{ConfigError, OutboundConfig, OutboundProtocolConfig};
pub(crate) const LEGACY_CONNECT_STAGE: &str = "connect_upstream_vless";
pub(crate) fn native_input(input: &OutboundConfig) -> bool {
    matches!(&input.protocol,OutboundProtocolConfig::Vless {
        mux_concurrency:None,xudp_concurrency:None,flow,testpre:0,
        final_mask,mkcp:None,hysteria:None,
        encryption,reality,quic:None,h2,grpc:None,http_upgrade,split_http:None,tls,ws,..
    } if (flow.as_deref().is_none_or(|flow|flow == "zero-aead-v1" ||
            matches!(flow,"xtls-rprx-vision"|"xtls-rprx-vision-udp443") && (tls.is_some() || reality.is_some() || encryption.as_deref().is_some_and(|value|value!="none")) && (encryption.as_deref().is_some_and(|value|value!="none") || (ws.is_none() && h2.is_none() && http_upgrade.is_none()))))
        && (reality.is_none() || (tls.is_none() && ws.is_none() && h2.is_none() && http_upgrade.is_none()))
        && final_mask.as_ref().is_none_or(|settings| settings.udp.is_empty())
        && ws.as_ref().is_none_or(|ws|ws.browser_dialer.is_none())
        && tls.as_ref().is_none_or(|tls|tls.ca_cert_path.is_none() && !tls.options.ech_config_list.contains("://")))
}
pub(crate) fn owned_input(input: &OutboundConfig) -> bool {
    let mut plain = input.clone();
    let OutboundProtocolConfig::Vless { tls, .. } = &mut plain.protocol else {
        return false;
    };
    if let Some(tls) = tls {
        if zero_transport::tls::OwnedPreparedClient::validate_profile(tls.as_ref()).is_err() {
            return false;
        }
        tls.ca_cert_path = None;
        if tls.options.ech_config_list.contains("://") {
            tls.options.ech_config_list.clear();
        }
    }
    native_input(&plain)
}
/// Existing root Browser UDP framing consumes its actual accepted Source.
/// Associated-prefix admission remains separate in relay_shape.
#[cfg(any(feature = "peer-raw-ip-runtime", test))]
pub(crate) fn browser_source_datagram(input: &OutboundConfig) -> bool {
    browser::eligible(input)
        && matches!(
            &input.protocol,
            OutboundProtocolConfig::Vless { flow, .. }
                if flow.as_deref().is_none_or(|value| !::vless::validation::is_vision_flow(value))
        )
}
/// Cold role grammar implemented by the actual factories and loan executors.
/// The returned facts do not stand in for a Host provider or close receipt.
#[cfg(any(feature = "peer-raw-ip-runtime", test))]
pub(crate) fn relay_shape(input: &OutboundConfig) -> Option<(bool, bool, bool)> {
    let OutboundProtocolConfig::Vless {
        flow,
        grpc,
        mux_concurrency,
        xudp_concurrency,
        ..
    } = &input.protocol
    else {
        return None;
    };
    if preconnect::eligible(input) {
        return Some((true, true, grpc.is_some()));
    }
    if mux::vision(input) && mux::eligible(input) {
        return Some((true, true, grpc.is_some()));
    }
    if owned_input(input) {
        return Some((
            flow.as_deref()
                .is_none_or(|value| !::vless::validation::is_vision_flow(value)),
            false,
            false,
        ));
    }
    if self::grpc::eligible(input) || mux::grpc_eligible(input) {
        return Some((true, true, true));
    }
    if browser::eligible(input) {
        // The existing Browser projection does not admit an associated Packet
        // prefix. Its ordinary business Datagram role grants no new grammar.
        return Some((false, true, false));
    }
    if quic::eligible(input) || xhttp::eligible(input) {
        return Some((
            flow.as_deref()
                .is_none_or(|value| !::vless::validation::is_vision_flow(value)),
            true,
            false,
        ));
    }
    if (mux_concurrency.is_some() || xudp_concurrency.is_some()) && mux::eligible(input) {
        return Some((true, true, grpc.is_some()));
    }
    None
}

pub(crate) fn component_options(
    input: &OutboundConfig,
) -> Result<::vless::component::Options, ConfigError> {
    let OutboundProtocolConfig::Vless { server, port, .. } = &input.protocol else {
        return Err(ConfigError::InvalidOutbound(format!(
            "outbound `{}` is not VLESS",
            input.tag
        )));
    };
    let options = super::outbound_options(&input.tag, (server, *port), &input.protocol)
        .expect("VLESS projection");
    ::vless::component::Options::from_options_refs(options, input.dial.to_policy()).map_err(
        |source| ConfigError::InvalidOutbound(format!("outbound `{}`: {source}", input.tag)),
    )
}
#[cfg(test)]
#[path = "component/tests.rs"]
mod tests;
