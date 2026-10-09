use crate::{OutboundConfig, OutboundProtocolConfig};

pub(super) fn validate_outbound_dial(outbound: &OutboundConfig) -> Result<(), String> {
    if !matches!(outbound.protocol, OutboundProtocolConfig::Direct) && !outbound.dial.is_default() {
        return Err("non-default dial constraints currently require a direct outbound".into());
    }
    outbound
        .dial
        .to_policy()
        .validate()
        .map_err(|error| format!("invalid dial constraints: {error}"))
}
