use zero_config::{ConfigError, OutboundConfig, OutboundProtocolConfig};
use zero_core::{request::Destination, Address};

pub(crate) const LEGACY_CONNECT_STAGE: &str = "connect_upstream_socks5";

pub(crate) fn component_options(
    input: &OutboundConfig,
) -> Result<::socks5::component::Options, ConfigError> {
    let OutboundProtocolConfig::Socks5 {
        server,
        port,
        username,
        password,
    } = &input.protocol
    else {
        return Err(ConfigError::InvalidOutbound(format!(
            "outbound `{}` is not SOCKS5",
            input.tag
        )));
    };
    let address = match zero_core::address::parse_ip_literal(server) {
        Some(std::net::IpAddr::V4(ip)) => Address::Ipv4(ip.octets()),
        Some(std::net::IpAddr::V6(ip)) => Address::Ipv6(ip.octets()),
        None => Address::Domain(server.clone()),
    };
    ::socks5::component::Options::new(
        Destination::new(address, *port),
        username.as_deref(),
        password.as_deref(),
        input.dial.to_policy(),
    )
    .map_err(|source| ConfigError::InvalidOutbound(format!("outbound `{}`: {source}", input.tag)))
}
