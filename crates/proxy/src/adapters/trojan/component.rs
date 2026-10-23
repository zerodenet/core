//! Own the cold projection from public Trojan config into protocol options.
//! gRPC stays with its existing driver until a native task/resource owner exists.
use zero_config::{ConfigError, OutboundConfig, OutboundProtocolConfig};
pub(crate) const LEGACY_CONNECT_STAGE: &str = "connect_upstream_trojan";

pub(crate) fn native_input(input: &OutboundConfig) -> bool {
    matches!(
        &input.protocol,
        OutboundProtocolConfig::Trojan {
            mux_concurrency: None,
            grpc: None,
            ws,
            ..
        } if ws.as_ref().is_none_or(|ws| ws.browser_dialer.is_none())
    )
}
pub(crate) fn component_options(
    input: &OutboundConfig,
) -> Result<::trojan::component::Options, ConfigError> {
    let OutboundProtocolConfig::Trojan { server, port, .. } = &input.protocol else {
        return Err(ConfigError::InvalidOutbound(format!(
            "outbound `{}` is not Trojan",
            input.tag
        )));
    };
    let options = super::outbound_options(&input.tag, (server, *port), &input.protocol)
        .expect("Trojan projection");
    ::trojan::component::Options::from_options_refs(options, input.dial.to_policy()).map_err(
        |source| ConfigError::InvalidOutbound(format!("outbound `{}`: {source}", input.tag)),
    )
}

#[cfg(test)]
#[path = "component/tests.rs"]
mod tests;
