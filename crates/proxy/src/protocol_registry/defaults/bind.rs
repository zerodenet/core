use zero_engine::EngineError;

use crate::protocol_registry::BoundInbound;

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
