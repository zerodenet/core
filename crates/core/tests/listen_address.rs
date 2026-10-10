use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use zero_core::address::{format_socket_addr, listen_hosts_overlap, parse_ip_literal, socket_addr};

#[test]
fn ipv6_literals_use_canonical_bracketed_socket_format() {
    for host in ["::1", "[::1]", "0:0:0:0:0:0:0:1", "[0:0:0:0:0:0:0:1]"] {
        assert_eq!(
            parse_ip_literal(host),
            Some(IpAddr::V6(Ipv6Addr::LOCALHOST))
        );
        assert_eq!(
            socket_addr(host, 443),
            Some(SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 443))
        );
        assert_eq!(format_socket_addr(host, 443), "[::1]:443");
    }
    assert_eq!(format_socket_addr("::", 0), "[::]:0");
    assert_eq!(format_socket_addr("[::]", 0), "[::]:0");
}

#[test]
fn ipv4_and_hostname_endpoint_support_is_preserved() {
    assert_eq!(format_socket_addr("127.0.0.1", 8080), "127.0.0.1:8080");
    assert_eq!(format_socket_addr("localhost", 8080), "localhost:8080");
    assert!(socket_addr("localhost", 8080).is_none());
    for malformed in ["[[::1]]", "[::1", "::1]", "[127.0.0.1]", "[localhost]"] {
        assert!(parse_ip_literal(malformed).is_none(), "{malformed}");
    }
}

#[test]
fn listen_overlap_normalizes_literals_and_conservatively_handles_wildcards() {
    for (left, right) in [
        ("::1", "[0:0:0:0:0:0:0:1]"),
        ("2001:DB8::1", "[2001:db8:0:0:0:0:0:1]"),
        ("127.0.0.1", "[::ffff:127.0.0.1]"),
        ("::ffff:7f00:1", "::ffff:127.0.0.1"),
        ("0.0.0.0", "127.0.0.1"),
        ("[::]", "2001:db8::1"),
        ("::", "0.0.0.0"),
        ("::", "127.0.0.1"),
        ("LOCALHOST", "localhost"),
    ] {
        assert!(listen_hosts_overlap(left, right), "{left} / {right}");
        assert!(listen_hosts_overlap(right, left), "{right} / {left}");
    }
    for (left, right) in [
        ("127.0.0.1", "127.0.0.2"),
        ("::1", "::2"),
        ("127.0.0.1", "::1"),
        ("0.0.0.0", "::1"),
        ("localhost", "example.test"),
    ] {
        assert!(!listen_hosts_overlap(left, right), "{left} / {right}");
    }
}
