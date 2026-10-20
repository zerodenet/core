//! Cold ordinary Stream selection; every excluded style retains its actual owner.
use zero_config::{ConfigError, OutboundConfig, OutboundProtocolConfig};
pub(crate) const LEGACY_CONNECT_STAGE: &str = "connect_upstream_vless";
pub(crate) fn native_input(input: &OutboundConfig) -> bool {
    matches!(&input.protocol,OutboundProtocolConfig::Vless {
        mux_concurrency:None,xudp_concurrency:None,flow,testpre:0,testseed,
        final_mask:None,mkcp:None,hysteria:None,reverse_tag:None,reverse_sniffing:None,
        encryption,reality:None,quic:None,h2:None,http_upgrade:None,split_http:None,tls,ws,grpc,..
    } if (flow.as_deref().map_or(testseed.is_empty(),|flow|
            matches!(flow,"xtls-rprx-vision"|"xtls-rprx-vision-udp443") && tls.is_some() && ws.is_none() && grpc.is_none()))
        && encryption.as_deref().is_none_or(|value|value=="none")
        && ws.as_ref().is_none_or(|ws|ws.browser_dialer.is_none())
        && tls.as_ref().is_none_or(|tls|tls.ca_cert_path.is_none() && !tls.options.ech_config_list.contains("://")))
}
pub(crate) fn component_options(
    input: &OutboundConfig,
) -> Result<::vless::component::Options, ConfigError> {
    let OutboundProtocolConfig::Vless {
        server,
        port,
        reverse_tag,
        reverse_sniffing,
        ..
    } = &input.protocol
    else {
        return Err(ConfigError::InvalidOutbound(format!(
            "outbound `{}` is not VLESS",
            input.tag
        )));
    };
    if reverse_tag.is_some() || reverse_sniffing.is_some() {
        return Err(ConfigError::InvalidOutbound(format!(
            "outbound `{}` reverse requires its owning lifecycle",
            input.tag
        )));
    }
    let options = super::outbound_options(&input.tag, (server, *port), &input.protocol)
        .expect("VLESS projection");
    ::vless::component::Options::from_options_refs(options, input.dial.to_policy()).map_err(
        |source| ConfigError::InvalidOutbound(format!("outbound `{}`: {source}", input.tag)),
    )
}
#[cfg(test)]
#[path = "component/tests.rs"]
mod tests;
