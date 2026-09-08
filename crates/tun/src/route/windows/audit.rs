use std::io;
use std::net::IpAddr;

use super::{create_route, host_prefix, matching_routes, SystemRouteGuard};

fn present(index: u32, prefix: &str, gateway: IpAddr) -> io::Result<bool> {
    let routes = matching_routes(index, prefix, None)?;
    routes_present(&routes, index, prefix, gateway)
}

fn routes_present(
    routes: &[windows_sys::Win32::NetworkManagement::IpHelper::MIB_IPFORWARD_ROW2],
    index: u32,
    prefix: &str,
    gateway: IpAddr,
) -> io::Result<bool> {
    if routes.is_empty() {
        return Ok(false);
    }
    if routes.iter().all(|row| {
        super::socket_ip(&row.NextHop).ok() == Some(gateway)
            && row.ValidLifetime != 0
            && !row.Loopback
    }) {
        Ok(true)
    } else {
        Err(io::Error::other(format!(
            "TUN route conflict for {prefix} on interface {index}"
        )))
    }
}

impl SystemRouteGuard {
    pub(super) fn audit_routes(&mut self) -> io::Result<bool> {
        let mut changed = false;
        for peer in self.excluded.clone() {
            let prefix = host_prefix(peer);
            if !present(self.egress.index(), &prefix, self.gateway)? {
                self.journal.record_exclusion(peer)?;
                create_route(self.egress.index(), &prefix, self.gateway)?;
                changed = true;
            }
        }
        for prefix in &self.journal.installed {
            if !present(self.tun_index, prefix, self.tun_gateway)? {
                self.add(prefix)?;
                changed = true;
            }
        }
        if changed {
            for peer in &self.excluded {
                verify(present(
                    self.egress.index(),
                    &host_prefix(*peer),
                    self.gateway,
                )?)?;
            }
            for prefix in &self.journal.installed {
                verify(present(self.tun_index, prefix, self.tun_gateway)?)?;
            }
        }
        Ok(changed)
    }
}

fn verify(present: bool) -> io::Result<()> {
    if present {
        Ok(())
    } else {
        Err(io::Error::other("TUN route still missing after repair"))
    }
}

#[cfg(test)]
mod tests;
