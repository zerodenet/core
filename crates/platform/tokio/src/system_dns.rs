use std::io;
use std::net::IpAddr;

#[cfg(target_os = "linux")]
#[path = "system_dns/linux.rs"]
mod platform;
#[cfg(target_os = "macos")]
#[path = "system_dns/macos.rs"]
mod platform;
#[cfg(target_os = "windows")]
#[path = "system_dns/windows.rs"]
mod platform;

/// Discover the non-local DNS endpoints used by the host resolver.
///
/// TUN strict-route mode installs explicit bypass routes for these endpoints
/// before publishing capture routes. Local stubs are intentionally rejected:
/// their real upstreams must be discovered so the stub cannot re-enter TUN.
pub fn system_dns_servers() -> io::Result<Vec<IpAddr>> {
    let mut servers = platform::system_dns_servers()?;
    servers.retain(|address| usable_upstream(*address));
    servers.sort_unstable();
    servers.dedup();
    servers.truncate(32);
    if servers.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "system DNS auto-discovery found no non-local upstream endpoints; configure an explicit UDP, DoH, DoT, or DoQ backend",
        ));
    }
    Ok(servers)
}

fn usable_upstream(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            !address.is_unspecified()
                && !address.is_loopback()
                && !address.is_link_local()
                && !address.is_multicast()
                && !address.is_broadcast()
        }
        IpAddr::V6(address) => {
            !address.is_unspecified()
                && !address.is_loopback()
                && !address.is_unicast_link_local()
                && !address.is_multicast()
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn parse_nameserver_lines(input: &str) -> Vec<IpAddr> {
    input
        .lines()
        .filter_map(|line| {
            let line = line.split('#').next()?.trim();
            let rest = line.strip_prefix("nameserver")?;
            let value = if rest.starts_with('[') {
                rest.split_once(':')?.1.trim()
            } else {
                rest.split_whitespace().next()?
            };
            let value = value.split('%').next().unwrap_or(value).trim();
            value.parse().ok()
        })
        .collect()
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
mod platform {
    use std::io;
    use std::net::IpAddr;

    pub(super) fn system_dns_servers() -> io::Result<Vec<IpAddr>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "system DNS auto-discovery is unsupported on this platform",
        ))
    }
}

#[cfg(test)]
mod tests;
