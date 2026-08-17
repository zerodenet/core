use std::io;
use std::net::IpAddr;

use serde::{Deserialize, Serialize};

mod journal;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
mod monitor;
mod reconcile;
#[cfg(target_os = "windows")]
mod windows;

use journal::{RouteJournal, RouteLease};
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
