use std::io;
use std::net::IpAddr;

use ipnet::IpNet;
use serde::{Deserialize, Serialize};

mod journal;
mod leak;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
mod monitor;
mod reconcile;
#[cfg(target_os = "windows")]
mod windows;

use journal::{RouteJournal, RouteLease};
pub use leak::SystemLeakGuard;
pub use monitor::RouteChangeMonitor;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteInterface {
    name: String,
    index: u32,
}

impl RouteInterface {
    pub(crate) fn new(name: String, index: u32) -> io::Result<Self> {
        if name.is_empty() || index == 0 {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "default route has no usable interface",
            ));
        }
        Ok(Self { name, index })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn index(&self) -> u32 {
        self.index
    }
}

#[cfg(target_os = "linux")]
pub use linux::SystemRouteGuard;
#[cfg(target_os = "macos")]
pub use macos::SystemRouteGuard;
#[cfg(target_os = "windows")]
pub use windows::SystemRouteGuard;

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
#[derive(Debug)]
pub struct SystemRouteGuard;

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
impl SystemRouteGuard {
    pub fn install(
        _tun_name: &str,
        _recovery_key: &str,
        _address: IpAddr,
        _netmask: IpAddr,
        _captured: &[IpNet],
        _excluded: &[IpAddr],
    ) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "automatic TUN routes are unsupported on this platform",
        ))
    }

    pub fn install_with_egress(
        _tun_name: &str,
        _recovery_key: &str,
        _address: IpAddr,
        _netmask: IpAddr,
        _captured: &[IpNet],
        _excluded: &[IpAddr],
        _publish_egress: impl FnOnce(&RouteInterface) -> io::Result<()>,
    ) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "automatic TUN routes are unsupported on this platform",
        ))
    }

    pub fn egress(&self) -> &RouteInterface {
        unreachable!("unsupported route guard cannot be constructed")
    }

    pub fn is_ipv6(&self) -> bool {
        unreachable!("unsupported route guard cannot be constructed")
    }

    pub fn reconcile(&mut self, _excluded: &[IpAddr]) -> io::Result<bool> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "automatic TUN routes are unsupported on this platform",
        ))
    }

    pub fn close(self) -> io::Result<()> {
        Ok(())
    }
}

pub fn split_default_route_prefixes(address: IpAddr) -> [&'static str; 2] {
    if address.is_ipv4() {
        ["0.0.0.0/1", "128.0.0.0/1"]
    } else {
        ["::/1", "8000::/1"]
    }
}

pub fn capture_route_prefixes(address: IpAddr, included: &[IpNet]) -> Vec<IpNet> {
    capture_route_prefixes_with_exclusions(address, included, &[])
}

/// Compile one address family's automatic capture routes from positive and
/// negative destination CIDRs. Exclusions are represented by splitting the
/// captured prefixes, so excluded traffic keeps the system route without a
/// second platform-specific bypass state.
pub fn capture_route_prefixes_with_exclusions(
    address: IpAddr,
    included: &[IpNet],
    excluded: &[IpNet],
) -> Vec<IpNet> {
    let mut prefixes: Vec<IpNet> = if included.is_empty() {
        split_default_route_prefixes(address)
            .into_iter()
            .map(|prefix| prefix.parse().expect("split-default CIDR is valid"))
            .collect()
    } else {
        included
            .iter()
            .copied()
            .filter(|prefix| prefix.addr().is_ipv6() == address.is_ipv6())
            .collect()
    };
    for excluded in excluded
        .iter()
        .copied()
        .filter(|prefix| prefix.addr().is_ipv6() == address.is_ipv6())
    {
        prefixes = prefixes
            .into_iter()
            .flat_map(|prefix| subtract_prefix(prefix, excluded))
            .collect();
    }
    prefixes.sort_unstable();
    prefixes.dedup();
    prefixes
}

fn subtract_prefix(captured: IpNet, excluded: IpNet) -> Vec<IpNet> {
    if captured.addr().is_ipv6() != excluded.addr().is_ipv6()
        || !captured.contains(&excluded.network())
    {
        return if excluded.contains(&captured.network()) {
            Vec::new()
        } else {
            vec![captured]
        };
    }
    if excluded.contains(&captured.network()) {
        return Vec::new();
    }
    split_prefix(captured)
        .into_iter()
        .flat_map(|child| subtract_prefix(child, excluded))
        .collect()
}

fn split_prefix(prefix: IpNet) -> [IpNet; 2] {
    let next = prefix.prefix_len() + 1;
    match prefix {
        IpNet::V4(prefix) => {
            let first = u32::from(prefix.network());
            let second = first | (1_u32 << (32 - next));
            [
                IpNet::new(IpAddr::V4(first.into()), next).expect("IPv4 child prefix is valid"),
                IpNet::new(IpAddr::V4(second.into()), next).expect("IPv4 child prefix is valid"),
            ]
        }
        IpNet::V6(prefix) => {
            let first = u128::from(prefix.network());
            let second = first | (1_u128 << (128 - next));
            [
                IpNet::new(IpAddr::V6(first.into()), next).expect("IPv6 child prefix is valid"),
                IpNet::new(IpAddr::V6(second.into()), next).expect("IPv6 child prefix is valid"),
            ]
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn command_error(program: &str, arguments: &[String], stderr: &[u8]) -> io::Error {
    io::Error::other(format!(
        "`{program} {}` failed: {}",
        arguments.join(" "),
        String::from_utf8_lossy(stderr).trim()
    ))
}

#[cfg(any(target_os = "linux", target_os = "windows"))]
fn host_prefix(address: IpAddr) -> String {
    format!("{address}/{}", if address.is_ipv4() { 32 } else { 128 })
}

fn family_exclusions(excluded: &[IpAddr], ipv6: bool) -> Vec<IpAddr> {
    let mut excluded = excluded
        .iter()
        .copied()
        .filter(|address| address.is_ipv6() == ipv6)
        .collect::<Vec<_>>();
    excluded.sort_unstable();
    excluded.dedup();
    excluded
}

#[cfg(test)]
mod tests;
