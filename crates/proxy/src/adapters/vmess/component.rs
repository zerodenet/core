//! Cold projection into actual standard Stream/Datagram owners, including gRPC.
pub(crate) mod grpc;
pub(crate) mod mux;
use zero_config::{ConfigError, OutboundConfig, OutboundProtocolConfig};
pub(crate) const LEGACY_CONNECT_STAGE: &str = "connect_upstream_vmess";

pub(crate) fn native_input(input: &OutboundConfig) -> bool {
    matches!(&input.protocol, OutboundProtocolConfig::Vmess {mux_concurrency:None,grpc:None,tls,ws,..}
        if ws.as_ref().is_none_or(|ws|ws.browser_dialer.is_none())
            && tls.as_ref().is_none_or(|tls|tls.ca_cert_path.is_none() && !tls.options.ech_config_list.contains("://")))
}
pub(crate) fn owned_input(input: &OutboundConfig) -> bool {
    let mut plain = input.clone();
    let OutboundProtocolConfig::Vmess { tls, .. } = &mut plain.protocol else {
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

impl super::VmessAdapter {
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
        let inner = ::vmess::component::Options::from_browser_options_refs(
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
