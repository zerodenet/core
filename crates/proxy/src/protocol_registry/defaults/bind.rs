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
