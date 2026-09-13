use crate::{ConfigError, FallbackConfig, FallbackDestinationConfig};
pub(super) fn validate(config: &FallbackConfig) -> Result<(), ConfigError> {
    let invalid = |message: &str| ConfigError::InvalidInbound(message.to_owned());
    if config.rules.is_empty() {
        if config.server.trim().is_empty() || config.port == 0 {
            return Err(invalid(
                "fallback requires a nonempty server and nonzero port",
            ));
        }
        return Ok(());
    }
    if !config.server.is_empty() || config.port != 0 || config.alpn.is_some() {
        return Err(invalid(
            "fallback.rules cannot be combined with the legacy single target",
        ));
    }
    for rule in &config.rules {
        if rule.proxy_protocol > 2 {
            return Err(invalid("fallback proxy_protocol must be 0, 1 or 2"));
        }
        if !rule.path.is_empty() && !rule.path.starts_with('/') {
            return Err(invalid("fallback path must be empty or start with /"));
        }
        match &rule.destination {
            FallbackDestinationConfig::Tcp { server, port }
                if server.trim().is_empty() || *port == 0 =>
            {
                return Err(invalid(
                    "fallback TCP destination requires a server and nonzero port",
                ))
            }
            FallbackDestinationConfig::Unix { path } if path.is_empty() || path.contains('\0') => {
                return Err(invalid(
                    "fallback Unix destination requires a nonempty path without NUL",
                ))
            }
            _ => {}
        }
    }
    Ok(())
}
