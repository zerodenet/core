use tokio::net::{TcpListener, TcpStream};
#[path = "process_lookup/owned.rs"]
mod owned;
use owned::LookupCaller;

async fn wait_for_tcp_process(
    caller: &mut LookupCaller,
    source: std::net::SocketAddr,
) -> Option<zero_platform_tokio::LocalProcessInfo> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
    loop {
        if let Some(process) = caller
            .lookup(source, zero_platform_tokio::LocalProcessTransport::Tcp)
            .await
        {
            return Some(process);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

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

    // Kernel socket-owner tables can lag immediately behind connect/accept.
    // Keep the live sockets open and wait briefly for the platform snapshot
    // instead of making the integration test depend on instant publication.
    let mut caller = LookupCaller::new().await;
    let process = wait_for_tcp_process(&mut caller, source)
        .await
        .expect("live TCP source must resolve to its owning process");
    assert_eq!(process.pid, std::process::id());
    assert!(process.name.as_deref().is_some_and(|name| !name.is_empty()));
    assert!(process.path.as_deref().is_some_and(|path| !path.is_empty()));
    caller.finish().await;
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn resolves_a_tcp_socket_beyond_the_darwin_descriptor_read_limit() {
    use std::{fs::File, num::NonZeroUsize, os::fd::AsRawFd};

    // Keep more than 512 lower descriptors open across the real lookup so
    // the client socket cannot be part of the 4 KiB PROC_PIDLISTFDS prefix.
    let files: Vec<_> = (0..530).map(|_| File::open("/dev/null").unwrap()).collect();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (_accepted, _) = listener.accept().await.unwrap();
    assert!(
        files
            .iter()
            .filter(|file| file.as_raw_fd() < client.as_raw_fd())
            .count()
            > 512,
        "the real client FD must be beyond the bounded descriptor prefix"
    );

    let mut caller = LookupCaller::new().await;
    let process = caller
        .lookup_with_limit(
            client.local_addr().unwrap(),
            zero_platform_tokio::LocalProcessTransport::Tcp,
            NonZeroUsize::new(4096).unwrap(),
        )
        .await
        .expect("bounded lookup must resolve the process owning the tail socket");
    assert_eq!(process.pid, std::process::id());
    caller.finish().await;
    drop(files);
}

#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "macos", windows))]
async fn resolves_the_process_that_owns_a_bound_udp_source() {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind UDP source");
    let source = socket.local_addr().expect("UDP source");

    let mut caller = LookupCaller::new().await;
    let process = caller
        .lookup(source, zero_platform_tokio::LocalProcessTransport::Udp)
        .await
        .expect("bound UDP source must resolve to its owning process");
    assert_eq!(process.pid, std::process::id());
    assert!(process.name.as_deref().is_some_and(|name| !name.is_empty()));
    assert!(process.path.as_deref().is_some_and(|path| !path.is_empty()));
    caller.finish().await;
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
    let mut caller = LookupCaller::new().await;
    assert_eq!(
        wait_for_tcp_process(&mut caller, tcp_source)
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
        caller
            .lookup(udp_source, zero_platform_tokio::LocalProcessTransport::Udp)
            .await
            .map(|info| info.pid),
        Some(std::process::id())
    );
    caller.finish().await;
}

#[test]
fn proc_net_matcher_skips_time_wait_zero_inodes_and_prefers_exact_ip_over_wildcard() {
    use zero_platform_tokio::proc_net::match_proc_net_row;

    let ip = "0100007F";
    let wildcard = "00000000";
    let port = "B26E";

    let time_wait =
        "  412: 0100007F:B26E 0100007F:8811 06 00000000:00000000 03:0000176F 00000000     0        0 0 3 0000000000000000";
    let wildcard_listen =
        "  500: 00000000:B26E 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1001        0 11111 1 0000000000000000 100 0 0 10 0";
    let established =
        " 1890: 0100007F:B26E 0100007F:9922 01 00000000:00000000 00:00000000 00000000  1001        0 482910 1 0000000000000000 20 4 30 10 -1";

    assert_eq!(match_proc_net_row(time_wait, ip, wildcard, port), None);
    assert_eq!(
        match_proc_net_row(wildcard_listen, ip, wildcard, port),
        Some((false, 11111))
    );
    assert_eq!(
        match_proc_net_row(established, ip, wildcard, port),
        Some((true, 482910))
    );
}
