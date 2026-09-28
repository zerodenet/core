//! Explicit host fixtures keep local IP tests independent of an active TUN.

use std::net::{IpAddr, Ipv4Addr, UdpSocket};

pub fn non_loopback_host_ipv4() -> Ipv4Addr {
    if let Ok(value) = std::env::var("ZERO_TEST_HOST_IPV4") {
        let address: Ipv4Addr = value
            .parse()
            .expect("ZERO_TEST_HOST_IPV4 must be an IPv4 address");
        assert!(
            !address.is_unspecified() && !address.is_loopback(),
            "test host must be a concrete non-loopback address"
        );
        UdpSocket::bind((address, 0)).expect("ZERO_TEST_HOST_IPV4 must be assigned to this host");
        return address;
    }
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.connect("192.0.2.1:9").unwrap();
    let IpAddr::V4(address) = socket.local_addr().unwrap().ip() else {
        panic!("IPv4 route required")
    };
    address
}
