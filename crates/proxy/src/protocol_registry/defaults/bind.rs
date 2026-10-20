use zero_engine::EngineError;

use crate::protocol_registry::BoundInbound;

/// Thin projection of an existing TCP listener declaration. Only adapters whose
/// bind shape is exactly one TCP listener opt in. Hostnames retain the legacy
/// resolving path until the provider has an explicit resolution contract.
pub(crate) fn prepare_tcp_inbound_bind(
    adapter: &dyn crate::protocol_registry::InboundListenerCapability,
    inbound: zero_config::InboundConfig,
    source_dir: Option<&std::path::Path>,
) -> Result<Option<crate::protocol_registry::PreparedInboundBind>, EngineError> {
    use zero_core::{request::Destination, Address};
    use zero_platform_tokio::contract::component::ResourceDeclaration;

    let Some(address) =
        zero_core::address::socket_addr(&inbound.listen.address, inbound.listen.port)
    else {
        return Ok(None);
    };
    let declaration = ResourceDeclaration::TcpListener(Destination::new(
        match address.ip() {
            std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
        },
        address.port(),
    ));
    let operation = adapter.prepare_inbound_listener(inbound, source_dir)?;
    crate::protocol_registry::PreparedInboundBind::new(
        operation,
        std::sync::Arc::from([declaration]),
    )
    .map(Some)
}

pub(crate) async fn bind_tcp_inbound(
    inbound: &zero_config::InboundConfig,
) -> Result<BoundInbound, EngineError> {
    let tcp = bind_tcp_listener(&inbound.listen.address, inbound.listen.port)
        .await
        .map_err(EngineError::Io)?;
    Ok(BoundInbound::Tcp(tcp))
}

pub(crate) async fn bind_tcp_listener(
    host: &str,
    port: u16,
) -> std::io::Result<zero_platform_tokio::TokioListener> {
    match zero_core::address::socket_addr(host, port) {
        Some(address) => zero_platform_tokio::TokioListener::bind_addr(address).await,
        None => {
            zero_platform_tokio::TokioListener::bind(&zero_core::address::format_socket_addr(
                host, port,
            ))
            .await
        }
    }
}

#[cfg(feature = "udp-runtime")]
pub(crate) async fn bind_datagram_listener(
    host: &str,
    port: u16,
) -> std::io::Result<tokio::net::UdpSocket> {
    match zero_core::address::socket_addr(host, port) {
        Some(address) => tokio::net::UdpSocket::bind(address).await,
        None => tokio::net::UdpSocket::bind((host, port)).await,
    }
}

pub(crate) fn inbound_listen_addr(inbound: &zero_config::InboundConfig) -> String {
    zero_core::address::format_socket_addr(&inbound.listen.address, inbound.listen.port)
}
