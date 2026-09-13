//! Thin target and rate-profile projection; REALITY owns the handshake policy.
pub(super) fn target(config: &zero_config::RealityTargetConfig) -> vless::reality::target::Profile {
    vless::reality::target::Profile {
        endpoint: match &config.destination {
            zero_config::FallbackDestinationConfig::Tcp { server, port } => {
                zero_traits::FallbackEndpoint::Tcp {
                    server: server.clone(),
                    port: *port,
                }
            }
            zero_config::FallbackDestinationConfig::Unix { path } => {
                zero_traits::FallbackEndpoint::Unix { path: path.clone() }
            }
        },
        proxy_protocol: config.proxy_protocol,
        upload: rate(config.upload),
        download: rate(config.download),
    }
}
fn rate(
    config: zero_config::RealityFallbackRateConfig,
) -> zero_transport::handshake_target::RateLimit {
    zero_transport::handshake_target::RateLimit {
        after_bytes: config.after_bytes,
        bytes_per_sec: config.bytes_per_sec,
        burst_bytes: config.burst_bytes,
    }
}
