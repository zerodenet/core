//! ECH URI metadata determines the real Node query/bootstrap dependencies.
use super::*;
pub(super) fn endpoint(
    input: &OutboundConfig,
) -> Result<Option<zero_dns::EchDnsEndpoint>, zero_config::ConfigError> {
    let OutboundProtocolConfig::Vless { tls: Some(tls), .. } = &input.protocol else {
        return Ok(None);
    };
    let options = tls.options.to_options();
    match options
        .ech
        .source()
        .map_err(|source| zero_config::ConfigError::InvalidOutbound(source.into()))?
    {
        Some(zero_traits::EchConfigSource::Dns { server, .. }) => {
            zero_dns::EchDnsEndpoint::parse(server)
                .map(Some)
                .map_err(|source| zero_config::ConfigError::InvalidOutbound(source.to_string()))
        }
        _ => Ok(None),
    }
}
pub(super) fn needs_ip_backend(input: &OutboundConfig) -> bool {
    matches!(&input.protocol,OutboundProtocolConfig::Vless{server,..} if zero_core::address::parse_ip_literal(server).is_none())
        || endpoint(input)
            .ok()
            .flatten()
            .is_some_and(|endpoint| endpoint.literal_peer().is_none())
}
