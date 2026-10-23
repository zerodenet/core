//! Pure selection and cold projection; resourceful styles keep their real owner.
//! gRPC stays with its existing driver until a native task/resource owner exists.
use zero_config::{ConfigError, OutboundConfig, OutboundProtocolConfig};
pub(crate) const LEGACY_CONNECT_STAGE: &str = "connect_upstream_vmess";

pub(crate) fn native_input(input: &OutboundConfig) -> bool {
    matches!(&input.protocol, OutboundProtocolConfig::Vmess {mux_concurrency:None,grpc:None,tls,ws,..}
        if ws.as_ref().is_none_or(|ws|ws.browser_dialer.is_none())
            && tls.as_ref().is_none_or(|tls|tls.ca_cert_path.is_none() && !tls.options.ech_config_list.contains("://")))
}
pub(crate) fn component_options(
    input: &OutboundConfig,
) -> Result<::vmess::component::Options, ConfigError> {
    let OutboundProtocolConfig::Vmess { server, port, .. } = &input.protocol else {
        return Err(ConfigError::InvalidOutbound(format!(
            "outbound `{}` is not VMess",
            input.tag
        )));
    };
    let options = super::outbound_options(&input.tag, (server, *port), &input.protocol)
        .expect("VMess projection");
    ::vmess::component::Options::from_options_refs(options, input.dial.to_policy()).map_err(
        |source| ConfigError::InvalidOutbound(format!("outbound `{}`: {source}", input.tag)),
    )
}

#[cfg(test)]
#[path = "component/tests.rs"]
mod tests;
