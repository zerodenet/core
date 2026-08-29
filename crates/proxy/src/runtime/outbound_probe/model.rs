use std::fmt;

use zero_core::Error as CoreError;
use zero_engine::EngineError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OutboundProbeError {
    code: &'static str,
    message: String,
}

impl OutboundProbeError {
    pub(super) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub(crate) fn code(&self) -> &'static str {
        self.code
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    /// Failures before the probe can reach the selected outbound do not
    /// provide evidence about that outbound's health.
    pub(crate) fn is_environmental_failure(&self) -> bool {
        let message = self.message.to_ascii_lowercase();
        message.contains("tun physical egress is unavailable")
            || message.contains("tun_ipv4_egress_unavailable")
            || message.contains("tun_ipv6_egress_unavailable")
            || message.contains("failed to resolve upstream target")
            || message.contains("failed to resolve proxy node")
            || message.contains("dns backend")
            || message.contains("tun route") && message.contains("unavailable")
    }

    pub(super) fn from_engine(error: EngineError) -> Self {
        let code = match &error {
            EngineError::Io(error) => match error.kind() {
                std::io::ErrorKind::TimedOut => "probe_timeout",
                std::io::ErrorKind::UnexpectedEof => "empty_response",
                std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::NotConnected => "connection_failed",
                _ => "probe_io_failed",
            },
            EngineError::Core(CoreError::Unsupported(_))
            | EngineError::CompiledFeatureDisabled { .. } => "unsupported_target",
            EngineError::Core(CoreError::Route(_))
            | EngineError::MissingRouteTarget { .. }
            | EngineError::SelectorGroupNotFound { .. }
            | EngineError::SelectorTargetNotFound { .. } => "target_resolution_failed",
            EngineError::Core(CoreError::Config(_)) | EngineError::Config(_) => "invalid_probe",
            EngineError::Core(CoreError::Protocol(_)) => "probe_protocol_failed",
            EngineError::Core(CoreError::Io(_)) => "connection_failed",
            EngineError::UnhealthyOutbound { .. } => "outbound_unhealthy",
            _ => "probe_failed",
        };
        Self::new(code, normalize_engine_error(&error))
    }
}

#[cfg(test)]
mod tests;

impl fmt::Display for OutboundProbeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for OutboundProbeError {}

#[derive(Clone)]
pub(crate) struct OutboundProbeRequest {
    pub(crate) url: String,
    pub(super) host: String,
    pub(super) port: u16,
    pub(super) request: String,
}

impl OutboundProbeRequest {
    pub(crate) fn parse(url: &str) -> Result<Self, OutboundProbeError> {
        let rest = url.strip_prefix("http://").ok_or_else(|| {
            OutboundProbeError::new(
                "invalid_probe_url",
                "outbound probe currently only supports `http://` URLs",
            )
        })?;
        let (authority, path) = match rest.split_once('/') {
            Some((authority, suffix)) => (authority, format!("/{suffix}")),
            None => (rest, "/".to_owned()),
        };
        if authority.trim().is_empty() {
            return Err(OutboundProbeError::new(
                "invalid_probe_url",
                "outbound probe URL requires a host",
            ));
        }

        let (host, port) = parse_authority(authority)?;
        let host_header = if port == 80 {
            authority.to_owned()
        } else if authority.contains(':') && !authority.starts_with('[') {
            format!("{host}:{port}")
        } else {
            authority.to_owned()
        };
        let request =
            format!("HEAD {path} HTTP/1.1\r\nHost: {host_header}\r\nConnection: close\r\n\r\n");
        Ok(Self {
            url: url.to_owned(),
            host,
            port,
            request,
        })
    }
}

fn normalize_engine_error(error: &EngineError) -> String {
    let message = error.to_string();
    message
        .strip_prefix("io error: ")
        .unwrap_or(message.as_str())
        .to_owned()
}

fn parse_authority(authority: &str) -> Result<(String, u16), OutboundProbeError> {
    let invalid_port =
        || OutboundProbeError::new("invalid_probe_url", "invalid port in outbound probe URL");
    if let Some(rest) = authority.strip_prefix('[') {
        let (host, port_part) = rest.split_once(']').ok_or_else(|| {
            OutboundProbeError::new(
                "invalid_probe_url",
                "invalid bracketed host in outbound probe URL",
            )
        })?;
        let port = match port_part.strip_prefix(':') {
            Some(port) => port.parse::<u16>().map_err(|_| invalid_port())?,
            None if port_part.is_empty() => 80,
            _ => {
                return Err(OutboundProbeError::new(
                    "invalid_probe_url",
                    "invalid authority in outbound probe URL",
                ))
            }
        };
        return Ok((host.to_owned(), port));
    }

    match authority.rsplit_once(':') {
        Some((host, port)) if !host.contains(':') => {
            Ok((host.to_owned(), port.parse().map_err(|_| invalid_port())?))
        }
        _ => Ok((authority.to_owned(), 80)),
    }
}
