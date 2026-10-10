use alloc::string::{String, ToString};
use core::net::{IpAddr, Ipv6Addr, SocketAddr};

/// Parse a bare IP literal or a single bracketed IPv6 literal.
///
/// Hostnames are deliberately left unresolved. Brackets are only valid around
/// IPv6, and malformed/nested brackets must not be silently stripped.
pub fn parse_ip_literal(host: &str) -> Option<IpAddr> {
    if let Some(host) = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
    {
        host.parse::<Ipv6Addr>().ok().map(IpAddr::V6)
    } else {
        host.parse().ok()
    }
}

/// Construct an IP socket endpoint without composing an ambiguous IPv6 string.
pub fn socket_addr(host: &str, port: u16) -> Option<SocketAddr> {
    parse_ip_literal(host).map(|ip| SocketAddr::new(ip, port))
}

/// Format an endpoint for APIs that take text, preserving hostname support.
pub fn format_socket_addr(host: &str, port: u16) -> String {
    match socket_addr(host, port) {
        Some(address) => address.to_string(),
        None => alloc::format!("{host}:{port}"),
    }
}

/// Whether two listen hosts can claim the same port on a supported OS.
///
/// IPv4-mapped IPv6 addresses are equivalent to their IPv4 address. IPv6
/// wildcard sockets may also claim IPv4 ports with the OS's default dual-stack
/// policy, so wildcard overlap is rejected conservatively and portably.
/// Hostname aliases are not resolved during configuration validation.
pub fn listen_hosts_overlap(a: &str, b: &str) -> bool {
    if a.eq_ignore_ascii_case(b) {
        return true;
    }
    match (parse_ip_literal(a), parse_ip_literal(b)) {
        (Some(a), Some(b)) => {
            let a = a.to_canonical();
            let b = b.to_canonical();
            a == b
                || ((a.is_unspecified() || b.is_unspecified()) && a.is_ipv4() == b.is_ipv4())
                || (a.is_ipv6() && a.is_unspecified())
                || (b.is_ipv6() && b.is_unspecified())
        }
        _ => false,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Address {
    Domain(String),
    Ipv4([u8; 4]),
    Ipv6([u8; 16]),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AddressFamily {
    Domain,
    Ipv4,
    Ipv6,
}

impl Address {
    pub fn family(&self) -> AddressFamily {
        match self {
            Self::Domain(_) => AddressFamily::Domain,
            Self::Ipv4(_) => AddressFamily::Ipv4,
            Self::Ipv6(_) => AddressFamily::Ipv6,
        }
    }
}
