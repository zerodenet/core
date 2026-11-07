pub(crate) fn component_options(
    inbound: &zero_config::InboundConfig,
) -> Result<http::component::InboundOptions, zero_config::ConfigError> {
    use std::num::NonZeroUsize;
    if !matches!(
        inbound.protocol,
        zero_config::InboundProtocolConfig::HttpConnect
    ) {
        return Err(zero_config::ConfigError::InvalidInbound(
            "HTTP factory received another protocol".into(),
        ));
    }
    let address = zero_core::address::socket_addr(&inbound.listen.address, inbound.listen.port);
    Ok(http::component::InboundOptions {
        listen: zero_core::request::Destination::new(
            match address.map(|address| address.ip()) {
                Some(std::net::IpAddr::V4(ip)) => zero_core::Address::Ipv4(ip.octets()),
                Some(std::net::IpAddr::V6(ip)) => zero_core::Address::Ipv6(ip.octets()),
                None => zero_core::Address::Domain(inbound.listen.address.clone()),
            },
            inbound.listen.port,
        ),
        accepted: zero_platform_tokio::contract::component::AcceptedTcpLimits {
            max_sockets: NonZeroUsize::new(4096).unwrap(),
            max_tasks_per_socket: NonZeroUsize::new(1).unwrap(),
            max_resources_per_socket: 0,
        },
        max_bind_attempts: NonZeroUsize::new(50).unwrap(),
    })
}

pub(crate) fn component_options_in_runtime(
    inbound: &zero_config::InboundConfig,
    runtime: &zero_config::RuntimeOptionsConfig,
) -> Result<http::component::InboundOptions, zero_config::ConfigError> {
    let mut options = component_options(inbound)?;
    options.accepted.max_tasks_per_socket =
        std::num::NonZeroUsize::new(runtime.effective_inbound_max_tasks_per_carrier(1)).unwrap();
    options.accepted.max_resources_per_socket =
        runtime.effective_inbound_max_resources_per_carrier(0);
    let limits = crate::adapters::source_budgets::limits(runtime, options.source_limits());
    options
        .with_inbound_source_limits(limits)
        .map_err(|source| zero_config::ConfigError::InvalidInbound(source.to_string()))
}
