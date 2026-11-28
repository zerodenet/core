//! Real paired loopback binds; a TCP reservation alone never reserves UDP.
pub(crate) fn tcp_udp() -> (std::net::TcpListener, std::net::UdpSocket) {
    tcp_udp_on(std::net::Ipv4Addr::LOCALHOST)
}

pub(crate) fn tcp_udp_on(
    address: std::net::Ipv4Addr,
) -> (std::net::TcpListener, std::net::UdpSocket) {
    tcp_udp_with(
        || std::net::TcpListener::bind((address, 0)).unwrap(),
        || std::net::UdpSocket::bind((address, 0)).unwrap(),
    )
}

fn tcp_udp_with(
    mut bind_tcp: impl FnMut() -> std::net::TcpListener,
    mut bind_udp: impl FnMut() -> std::net::UdpSocket,
) -> (std::net::TcpListener, std::net::UdpSocket) {
    let mut last = None;
    for attempt in 0..16 {
        // bind(0) avoids only its own protocol's excluded ports. Alternate
        // allocators instead of walking a TCP-only or UDP-only exclusion hole.
        let (candidate, source) = if attempt % 2 == 0 {
            let tcp = bind_tcp();
            let candidate = tcp.local_addr().unwrap();
            match std::net::UdpSocket::bind(candidate) {
                Ok(udp) => return (tcp, udp),
                Err(source) => (candidate, source),
            }
        } else {
            let udp = bind_udp();
            let candidate = udp.local_addr().unwrap();
            match std::net::TcpListener::bind(candidate) {
                Ok(tcp) => return (tcp, udp),
                Err(source) => (candidate, source),
            }
        };
        if source.kind() != std::io::ErrorKind::AddrInUse
            && !(cfg!(windows) && source.raw_os_error() == Some(10013))
        {
            panic!("bind actual paired fixture address {candidate}, attempt {attempt}: {source}");
        }
        // Windows reports an excluded or exclusively occupied candidate as
        // WSAEACCES (10013). Only the second, fixed-address bind is retried.
        last = Some((candidate, source));
    }
    panic!("no shared TCP/UDP fixture address after 16 actual bind attempts; last={last:?}")
}

#[path = "socket_fixture/tests.rs"]
mod tests;

#[path = "../tests/support/host.rs"]
mod host;
pub(crate) use host::non_loopback_host_ipv4;
