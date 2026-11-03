//! Real paired loopback binds; a TCP reservation alone never reserves UDP.
pub(crate) fn tcp_udp() -> (std::net::TcpListener, std::net::UdpSocket) {
    for _ in 0..16 {
        let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        match std::net::UdpSocket::bind(tcp.local_addr().unwrap()) {
            Ok(udp) => return (tcp, udp),
            Err(source) if source.kind() == std::io::ErrorKind::AddrInUse => continue,
            Err(source) => panic!("bind actual paired UDP fixture address: {source}"),
        }
    }
    panic!("no shared TCP/UDP fixture address after 16 actual bind attempts")
}
