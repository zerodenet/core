use std::io;

use ipnet::IpNet;
use serde::Deserialize;

use super::{add_exclusion, family_arguments, host_prefix, run_ip, SystemRouteGuard};

#[derive(Debug, Deserialize)]
struct Route {
    dst: String,
    dev: Option<String>,
    gateway: Option<String>,
    #[serde(default)]
    flags: Vec<String>,
    #[serde(rename = "type", default)]
    kind: Option<String>,
}

fn present(ipv6: bool, prefix: &str, dev: &str, gateway: Option<&str>) -> io::Result<bool> {
    let mut args = family_arguments(ipv6);
    args.extend(["-j", "route", "show", "table", "main", "exact", prefix].map(str::to_owned));
    let routes: Vec<Route> = serde_json::from_slice(&run_ip(&args)?).map_err(io::Error::other)?;
    matches(&routes, prefix, dev, gateway)
}

fn matches(routes: &[Route], prefix: &str, dev: &str, gateway: Option<&str>) -> io::Result<bool> {
    if routes.is_empty() {
        return Ok(false);
    }
    let desired: IpNet = prefix.parse().map_err(io::Error::other)?;
    if routes.iter().all(|route| {
        let destination = route.dst.parse::<IpNet>().ok().or_else(|| {
            route
                .dst
                .parse()
                .ok()
                .and_then(|address: std::net::IpAddr| {
                    IpNet::new(address, if address.is_ipv6() { 128 } else { 32 }).ok()
                })
        });
        destination == Some(desired)
            && route.dev.as_deref() == Some(dev)
            && route.gateway.as_deref() == gateway
            && !route
                .flags
                .iter()
                .any(|flag| flag == "linkdown" || flag == "dead")
            && route.kind.as_deref().is_none_or(|kind| kind == "unicast")
    }) {
        Ok(true)
    } else {
        Err(io::Error::other(format!(
            "TUN route conflict for {prefix} on {dev}: {routes:?}"
        )))
    }
}

impl SystemRouteGuard {
    pub(super) fn audit_routes(&mut self) -> io::Result<bool> {
        let mut changed = false;
        // Restore bypasses before capture routes. A partial failure stays in
        // the journal and is retried; never delete a competing OS route.
        for peer in self.excluded.clone() {
            let prefix = host_prefix(peer);
            if !present(
                self.ipv6,
                &prefix,
                self.egress.name(),
                self.gateway.as_deref(),
            )? {
                self.journal.record_exclusion(peer)?;
                add_exclusion(self.ipv6, &self.egress, self.gateway.as_deref(), peer)?;
                changed = true;
            }
        }
        for prefix in &self.journal.installed {
            if !present(self.ipv6, prefix, &self.tun_name, None)? {
                self.add(prefix)?;
                changed = true;
            }
        }
        // Read back every required route after mutations. A successful add
        // command alone is not evidence that concurrent network changes kept it.
        if changed {
            for peer in &self.excluded {
                verify(present(
                    self.ipv6,
                    &host_prefix(*peer),
                    self.egress.name(),
                    self.gateway.as_deref(),
                )?)?;
            }
            for prefix in &self.journal.installed {
                verify(present(self.ipv6, prefix, &self.tun_name, None)?)?;
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
