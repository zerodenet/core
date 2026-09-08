use std::io;
use std::net::IpAddr;

use ipnet::IpNet;

use super::{add_exclusion, gateway_matches_family, run_route, SystemRouteGuard};

mod table;
use table::RouteEntry;

#[derive(Debug, Clone, Copy)]
enum RouteKind {
    Scoped,
    Exclusion(IpAddr),
    Capture,
}

#[derive(Debug, Clone)]
struct ExpectedRoute {
    kind: RouteKind,
    prefix: IpNet,
    interface: String,
    gateway: Option<String>,
}

impl ExpectedRoute {
    fn present(&self, table: &[RouteEntry]) -> io::Result<bool> {
        let scoped = matches!(self.kind, RouteKind::Scoped);
        let matching: Vec<_> = table
            .iter()
            .filter(|row| {
                row.prefix == self.prefix
                    && row.flags.contains('I') == scoped
                    && (!scoped || row.interface == self.interface)
                    // ARP/NDP cache entries aren't owned host routes.
                    && !row.flags.contains('L')
            })
            .collect();
        if matching.is_empty() {
            return Ok(false);
        }
        if matching.iter().all(|row| {
            row.interface == self.interface
                && row.flags.contains('U')
                && !row.flags.contains(['R', 'B'])
                && self.gateway.as_ref().map_or_else(
                    || !row.flags.contains('G'),
                    |gateway| &row.gateway == gateway,
                )
        }) {
            return Ok(true);
        }
        // A competing route must not be silently adopted or overwritten. The
        // journal records what we created, not ownership of a replacement by
        // another VPN or the OS. Keep protection and expose the conflict.
        Err(io::Error::other(format!(
            "TUN route conflict for {} on {} (expected gateway {:?}, observed {:?})",
            self.prefix, self.interface, self.gateway, matching
        )))
    }
}

fn reconcile_with(
    expected: &[ExpectedRoute],
    mut read: impl FnMut() -> io::Result<Vec<RouteEntry>>,
    mut install: impl FnMut(&ExpectedRoute) -> io::Result<()>,
) -> io::Result<bool> {
    let before = read()?;
    // Check all conflicts before installing anything. Repeated healthy audits
    // issue no route mutations and do not create notification feedback loops.
    let missing = expected
        .iter()
        .map(|route| {
            route
                .present(&before)
                .map(|present| (!present).then_some(route))
        })
        .collect::<io::Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(false);
    }
    for route in &missing {
        install(route)?;
    }
    let after = read()?;
    for route in expected {
        if !route.present(&after)? {
            return Err(io::Error::other(format!(
                "TUN route verification failed: {} on {} is still missing",
                route.prefix, route.interface
            )));
        }
    }
    Ok(true)
}

impl SystemRouteGuard {
    pub(super) fn audit_routes(&mut self) -> io::Result<bool> {
        let mut expected = Vec::new();
        if gateway_matches_family(self.ipv6, self.gateway.as_deref()) {
            expected.push(ExpectedRoute {
                kind: RouteKind::Scoped,
                prefix: if self.ipv6 { "::/0" } else { "0.0.0.0/0" }
                    .parse()
                    .unwrap(),
                interface: self.egress.name().into(),
                gateway: self.gateway.clone(),
            });
        }
        expected.extend(self.excluded.iter().map(|peer| ExpectedRoute {
            kind: RouteKind::Exclusion(*peer),
            prefix: IpNet::new(*peer, if peer.is_ipv6() { 128 } else { 32 }).unwrap(),
            interface: self.egress.name().into(),
            gateway: self.gateway.clone(),
        }));
        expected.extend(self.captured.iter().map(|prefix| ExpectedRoute {
            kind: RouteKind::Capture,
            prefix: *prefix,
            interface: self.tun_name.clone(),
            gateway: self.tun_gateway.clone(),
        }));
        let ipv6 = self.ipv6;
        reconcile_with(
            &expected,
            || table::read(ipv6),
            |route| {
                match route.kind {
                    RouteKind::Scoped => {
                        // Persist ownership before creating a missing route. Keep
                        // it on failure so partial recovery remains cleanable.
                        self.journal.record_scoped_bypass()?;
                        run_route(&super::scoped::scoped_bypass_add_arguments(
                            self.ipv6,
                            self.egress.name(),
                            self.gateway.as_deref(),
                        ))?;
                    }
                    RouteKind::Exclusion(peer) => {
                        self.journal.record_exclusion(peer)?;
                        add_exclusion(&self.egress, self.gateway.as_deref(), self.ipv6, peer)?;
                    }
                    RouteKind::Capture => self.add(&route.prefix.to_string())?,
                }
                tracing::info!(prefix = %route.prefix, interface = %route.interface,
                "restored missing TUN route");
                Ok(())
            },
        )
    }
}

#[cfg(test)]
mod tests;
