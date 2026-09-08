//! HTTP/3 authentication negotiation, independent of HTTP and QUIC runtimes.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiveBandwidth {
    /// Require adaptive congestion control, even when a local rate is configured.
    Auto,
    /// Bytes per second; zero means no advertised receive limit.
    Limit(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthResponse {
    pub udp_enabled: bool,
    pub receive_bandwidth: ReceiveBandwidth,
}

impl AuthResponse {
    /// Match the reference implementation's permissive missing/invalid-header defaults.
    pub fn from_headers(udp: Option<&str>, receive_bandwidth: Option<&str>) -> Self {
        let udp_enabled = matches!(udp, Some("1" | "t" | "T" | "TRUE" | "true" | "True"));
        let receive_bandwidth = match receive_bandwidth {
            Some("auto") => ReceiveBandwidth::Auto,
            Some(value) if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) => {
                ReceiveBandwidth::Limit(value.parse().unwrap_or(u64::MAX))
            }
            _ => ReceiveBandwidth::Limit(0),
        };
        Self {
            udp_enabled,
            receive_bandwidth,
        }
    }
}
