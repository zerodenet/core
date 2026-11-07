//! Configuration projection only; the protocol owns authentication and frames.
use socks5::{component::listener::InboundOptions, transport::Socks5InboundUserRef};
use std::num::NonZeroUsize;
use zero_config::{ConfigError, InboundConfig, InboundProtocolConfig};
pub(crate) fn component_options(inbound: &InboundConfig) -> Result<InboundOptions, ConfigError> {
    let InboundProtocolConfig::Socks5 { users } = &inbound.protocol else {
        return Err(ConfigError::InvalidInbound(
            "SOCKS5 factory received another protocol".into(),
        ));
    };
    options(inbound, users)
}
pub(in crate::adapters) fn options(
    inbound: &InboundConfig,
    users: &[zero_config::Socks5UserConfig],
) -> Result<InboundOptions, ConfigError> {
    let literal = zero_core::address::socket_addr(&inbound.listen.address, inbound.listen.port);
    let listen = zero_core::request::Destination::new(
        match literal.map(|address| address.ip()) {
            Some(std::net::IpAddr::V4(ip)) => zero_core::Address::Ipv4(ip.octets()),
            Some(std::net::IpAddr::V6(ip)) => zero_core::Address::Ipv6(ip.octets()),
            None => zero_core::Address::Domain(inbound.listen.address.clone()),
        },
        inbound.listen.port,
    );
    InboundOptions::new(
        listen,
        zero_platform_tokio::contract::component::AcceptedTcpLimits {
            max_sockets: NonZeroUsize::new(4096).unwrap(),
            max_tasks_per_socket: NonZeroUsize::new(2).unwrap(),
            max_resources_per_socket: 2,
        },
        NonZeroUsize::new(50).unwrap(),
        NonZeroUsize::new(1).unwrap(),
        users.iter().map(|user| Socks5InboundUserRef {
            username: &user.username,
            password: &user.password,
            principal_key: user.principal_key.as_deref(),
            up_bps: user.up_bps,
            down_bps: user.down_bps,
        }),
    )
    .map_err(|source| ConfigError::InvalidInbound(source.to_string()))
}

pub(crate) fn component_options_in_runtime(
    inbound: &zero_config::InboundConfig,
    runtime: &zero_config::RuntimeOptionsConfig,
) -> Result<socks5::component::listener::InboundOptions, zero_config::ConfigError> {
    component_options(inbound)?
        .with_inbound_work_limits(
            std::num::NonZeroUsize::new(runtime.effective_inbound_max_tasks_per_carrier(2))
                .unwrap(),
            runtime.effective_inbound_max_resources_per_carrier(2),
        )
        .map_err(|source| zero_config::ConfigError::InvalidInbound(source.to_string()))
}
