use zero_config::{InboundConfig, InboundProtocolConfig};
pub(crate) fn options(
    input: &InboundConfig,
) -> Result<::mieru::component::inbound::Options, crate::component::RegistrationError> {
    let InboundProtocolConfig::Mieru {
        users,
        options,
        transport,
    } = &input.protocol
    else {
        return Err(zero_config::ConfigError::InvalidInbound(
            "Mieru source requires Mieru configuration".into(),
        )
        .into());
    };
    let profile =
        ::mieru::inbound::MieruInboundProfile::from_config_users(users.iter().map(|user| {
            (
                user.username.as_str(),
                user.password.as_str(),
                user.principal_key.as_deref(),
            )
        }))
        .with_options(options.clone());
    let endpoint = zero_core::address::socket_addr(&input.listen.address, input.listen.port);
    let address = match endpoint.map(|address| address.ip()) {
        Some(std::net::IpAddr::V4(ip)) => zero_core::Address::Ipv4(ip.octets()),
        Some(std::net::IpAddr::V6(ip)) => zero_core::Address::Ipv6(ip.octets()),
        None => zero_core::Address::Domain(input.listen.address.clone()),
    };
    let identity = serde_json::to_vec(input)
        .map_err(|source| zero_config::ConfigError::InvalidInbound(source.to_string()))?;
    ::mieru::component::inbound::Options::new(
        zero_core::request::Destination::new(address, input.listen.port),
        matches!(transport, zero_config::MieruTransport::Udp),
        profile,
        &identity,
    )
    .map_err(Into::into)
}
