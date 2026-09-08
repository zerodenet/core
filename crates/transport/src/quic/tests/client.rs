use super::wildcard_bind_addr;
use std::net::SocketAddr;

#[test]
fn uses_ipv4_wildcard_for_ipv4_server() {
    let server: SocketAddr = "192.0.2.1:443".parse().unwrap();

    assert_eq!(wildcard_bind_addr(server), "0.0.0.0:0".parse().unwrap());
}

#[test]
fn uses_ipv6_wildcard_for_ipv6_server() {
    let server: SocketAddr = "[2001:db8::1]:443".parse().unwrap();

    assert_eq!(wildcard_bind_addr(server), "[::]:0".parse().unwrap());
}
