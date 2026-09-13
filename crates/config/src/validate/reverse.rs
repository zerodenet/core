use crate::{ConfigError, InboundProtocolConfig, OutboundProtocolConfig, RuntimeConfig};
use std::collections::HashSet;

pub(super) fn validate(
    config: &RuntimeConfig,
    inbound_tags: &mut HashSet<String>,
) -> Result<(), ConfigError> {
    let mut portal_users = HashSet::new();
    for inbound in &config.inbounds {
        let InboundProtocolConfig::Vless { users, .. } = &inbound.protocol else {
            continue;
        };
        for user in users {
            let Some(tag) = &user.reverse_tag else {
                continue;
            };
            if !config.outbounds.iter().any(|outbound| {
                outbound.tag() == tag
                    && matches!(outbound.protocol, OutboundProtocolConfig::VlessReverse)
            }) {
                return Err(ConfigError::InvalidInbound(format!(
                    "inbound `{}` reverse_tag `{tag}` must reference a vless_reverse outbound",
                    inbound.tag
                )));
            }
            if !portal_users.insert(tag) {
                return Err(ConfigError::InvalidInbound(format!(
                    "reverse_tag `{tag}` must belong to one authenticated user"
                )));
            }
        }
    }
    for outbound in &config.outbounds {
        let OutboundProtocolConfig::Vless {
            reverse_tag,
            reverse_sniffing,
            ..
        } = &outbound.protocol
        else {
            continue;
        };
        if let Some(sniffing) = reverse_sniffing {
            if reverse_tag.is_none() {
                return Err(ConfigError::InvalidOutbound(format!(
                    "outbound `{}`: reverse_sniffing requires reverse_tag",
                    outbound.tag()
                )));
            }
            for protocol in &sniffing.destination_override {
                if !matches!(
                    protocol.to_ascii_lowercase().as_str(),
                    "http" | "tls" | "https" | "ssl" | "quic" | "fakedns" | "fakedns+others"
                ) {
                    return Err(ConfigError::InvalidOutbound(format!(
                        "outbound `{}`: reverse_sniffing has unknown destination override `{protocol}`",
                        outbound.tag()
                    )));
                }
            }
            for excluded in &sniffing.domains_excluded {
                let excluded = excluded.to_ascii_lowercase();
                if let Some(pattern) = excluded.strip_prefix("regexp:") {
                    zero_router::CompiledRegex::new(pattern.to_owned()).map_err(|error| {
                        ConfigError::InvalidOutbound(format!(
                            "outbound `{}`: reverse_sniffing domain exclusion `{excluded}` is invalid: {error}",
                            outbound.tag()
                        ))
                    })?;
                }
            }
        }
        let Some(tag) = reverse_tag else {
            continue;
        };
        super::validate_tag("reverse inbound", tag, inbound_tags).map_err(|error| {
            ConfigError::InvalidOutbound(format!("outbound `{}`: {error}", outbound.tag()))
        })?;
    }
    Ok(())
}
