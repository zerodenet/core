use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "macos", windows))]
async fn resolves_the_process_that_owns_a_live_tcp_source() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let listener_addr = listener.local_addr().expect("listener address");
    let client = TcpStream::connect(listener_addr)
        .await
        .expect("connect client");
    let source = client.local_addr().expect("client source");
    let (_accepted, _) = listener.accept().await.expect("accept client");

    let process = zero_platform_tokio::lookup_local_tcp_process(source)
        .await
        .expect("live TCP source must resolve to its owning process");
    assert_eq!(process.pid, std::process::id());
    assert!(process.name.as_deref().is_some_and(|name| !name.is_empty()));
    assert!(process.path.as_deref().is_some_and(|path| !path.is_empty()));
}

#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "macos", windows))]
async fn resolves_the_process_that_owns_a_bound_udp_source() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind UDP source");
    let source = socket.local_addr().expect("UDP source");

    let process = zero_platform_tokio::lookup_local_udp_process(source)
        .await
        .expect("bound UDP source must resolve to its owning process");
    assert_eq!(process.pid, std::process::id());
    assert!(process.name.as_deref().is_some_and(|name| !name.is_empty()));
    assert!(process.path.as_deref().is_some_and(|path| !path.is_empty()));
}

#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "macos", windows))]
async fn resolves_ipv6_tcp_and_udp_sources() {
    let listener = TcpListener::bind("[::1]:0").await.expect("bind IPv6 TCP");
    let listener_addr = listener.local_addr().expect("IPv6 listener address");
    let client = TcpStream::connect(listener_addr)
        .await
        .expect("connect IPv6 client");
    let tcp_source = client.local_addr().expect("IPv6 TCP source");
    let (_accepted, _) = listener.accept().await.expect("accept IPv6 client");
    assert_eq!(
        zero_platform_tokio::lookup_local_tcp_process(tcp_source)
            .await
            .map(|info| info.pid),
        Some(std::process::id())
    );

    let udp = tokio::net::UdpSocket::bind("[::]:0")
        .await
        .expect("bind IPv6 UDP");
    let udp_source = std::net::SocketAddr::new(
        "::1".parse().expect("IPv6 loopback"),
        udp.local_addr().expect("IPv6 UDP source").port(),
    );
    assert_eq!(
        zero_platform_tokio::lookup_local_udp_process(udp_source)
            .await
            .map(|info| info.pid),
        Some(std::process::id())
    );
}
