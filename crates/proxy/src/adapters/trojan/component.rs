//! Own the cold projection from public Trojan config into protocol options.
//! Inline gRPC uses the same controlled original; MUX retains its actual pool.
pub(crate) mod mux;
use zero_config::{ConfigError, OutboundConfig, OutboundProtocolConfig};
pub(crate) const LEGACY_CONNECT_STAGE: &str = "connect_upstream_trojan";

pub(crate) fn native_input(input: &OutboundConfig) -> bool {
    matches!(
        &input.protocol,
        OutboundProtocolConfig::Trojan {
            mux_concurrency: None,
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

impl super::TrojanAdapter {
    pub(crate) fn browser_options(
        &self,
        input: &OutboundConfig,
        resources: &mut crate::component::ResourcePreparation<'_>,
        network: std::sync::Arc<crate::component::background::BackgroundNetwork>,
    ) -> Result<mux::Options, crate::component::RegistrationError> {
        let carrier = crate::adapters::browser::prepare(input, resources, &self.browser)?;
        let refs = super::outbound_options(
            &input.tag,
            (&carrier.server, carrier.endpoint.port),
            &input.protocol,
        )
        .expect("actual Browser protocol projection");
        let inner = ::trojan::component::Options::from_browser_options_refs(
            refs,
            input.dial.to_policy(),
            carrier.clone(),
        )
        .map_err(|e| ConfigError::InvalidOutbound(e.to_string()))?;
        Ok(mux::Options::Browser(std::sync::Arc::new(
            crate::adapters::browser::Options::new(inner, carrier, input.tag.clone(), network),
        )))
    }
}
