use std::io;
use std::net::IpAddr;
use std::process::Command;

use ipnet::IpNet;

use super::reconcile::{reconcile_route_state, with_rollback_error, RouteReconcileState};
use super::{
    command_error, family_exclusions, host_prefix, EgressUnavailableReason, FamilyEgressState,
    RouteInterface, RouteJournal, RouteLease,
};

#[derive(Debug)]
pub struct SystemRouteGuard {
    /// Carrier used by route installation and exclusion maintenance.
    egress: RouteInterface,
    /// Native socket reachability for the TUN address family.
    family_egress: FamilyEgressState,
    tun_name: String,
    ipv6: bool,
    gateway: Option<String>,
    excluded: Vec<IpAddr>,
    journal: RouteJournal,
}

#[derive(Debug)]
struct LinuxRoute {
    dev: String,
    gateway: Option<String>,
    metric: u32,
}

struct LinuxEgressSelection {
    carrier: RouteInterface,
    gateway: Option<String>,
    family: FamilyEgressState,
}

impl SystemRouteGuard {
    pub fn install(
        tun_name: &str,
        recovery_key: &str,
        address: IpAddr,
        netmask: IpAddr,
        captured: &[IpNet],
        excluded: &[IpAddr],
    ) -> io::Result<Self> {
        Self::install_with_egress(
            tun_name,
            recovery_key,
            address,
            netmask,
            captured,
            excluded,
            |_| Ok(()),
        )
    }

    pub fn install_with_egress(
        tun_name: &str,
        recovery_key: &str,
        address: IpAddr,
        _netmask: IpAddr,
        captured: &[IpNet],
        excluded: &[IpAddr],
        publish_egress: impl FnOnce(&FamilyEgressState) -> io::Result<()>,
    ) -> io::Result<Self> {
        let ipv6 = address.is_ipv6();
        let lease = RouteLease::acquire(recovery_key, ipv6)?;
        recover_stale_routes(&lease, ipv6)?;
        let has_family_exclusions = excluded.iter().any(|peer| peer.is_ipv6() == ipv6);
        let selected = select_physical_egress(ipv6, tun_name, !has_family_exclusions)?;
        let egress = selected.carrier;
        let gateway = selected.gateway;
        let journal = RouteJournal::new(lease, tun_name, ipv6, 0, egress.clone(), gateway.clone())?;
        let desired_exclusions = family_exclusions(excluded, ipv6);
        let mut guard = Self {
            egress,
            family_egress: selected.family,
            tun_name: tun_name.to_owned(),
            ipv6,
            gateway,
            excluded: desired_exclusions.clone(),
            journal,
        };
        publish_egress(&guard.family_egress)?;
        for peer in desired_exclusions {
            guard.install_exclusion(peer)?;
        }
        for prefix in captured {
            let prefix = prefix.to_string();
            let _ = guard.remove(&prefix);
            guard.add(&prefix)?;
            guard.journal.record_route(&prefix)?;
        }
        Ok(guard)
    }

    pub fn egress(&self) -> &RouteInterface {
        &self.egress
    }

    pub fn family_egress(&self) -> &FamilyEgressState {
        &self.family_egress
    }

    pub fn is_ipv6(&self) -> bool {
        self.ipv6
    }

    /// Re-resolve the preferred physical interface and reconcile explicit
    /// bypass routes without replacing the TUN device or split default routes.
    pub fn reconcile(&mut self, excluded: &[IpAddr]) -> io::Result<bool> {
        let desired_exclusions = family_exclusions(excluded, self.ipv6);
        let has_family_exclusions = !desired_exclusions.is_empty();
        let selected = select_physical_egress(self.ipv6, &self.tun_name, !has_family_exclusions)?;
        let family_changed = self.family_egress != selected.family;
        let changed =
            reconcile_route_state(self, selected.carrier, selected.gateway, desired_exclusions)?;
        self.family_egress = selected.family;
        Ok(changed || family_changed)
    }

    pub fn close(mut self) -> io::Result<()> {
        self.cleanup()
    }

    fn add(&self, prefix: &str) -> io::Result<()> {
        let mut arguments = family_arguments(self.ipv6);
        arguments.extend([
            "route".to_owned(),
            "add".to_owned(),
            prefix.to_owned(),
            "dev".to_owned(),
            self.tun_name.clone(),
            "metric".to_owned(),
            "0".to_owned(),
        ]);
        run_ip(&arguments).map(|_| ())
    }

    fn remove(&self, prefix: &str) -> io::Result<()> {
        remove_route(self.ipv6, &self.tun_name, prefix)
    }

    fn install_exclusion(&mut self, peer: IpAddr) -> io::Result<()> {
        self.journal.record_exclusion(peer)?;
        if let Err(error) = add_exclusion(self.ipv6, &self.egress, self.gateway.as_deref(), peer) {
            let rollback = self.journal.forget_exclusion(peer);
            return Err(with_rollback_error(error, rollback));
        }
        Ok(())
    }

    fn reconcile_exclusions(&mut self, desired: &[IpAddr]) -> io::Result<()> {
        let added = desired
            .iter()
            .copied()
            .filter(|peer| !self.excluded.contains(peer))
            .collect::<Vec<_>>();
        for peer in added {
            self.install_exclusion(peer)?;
        }
        let stale = self
            .excluded
            .clone()
            .into_iter()
            .filter(|peer| !desired.contains(peer) && self.journal.excluded.contains(peer))
            .collect::<Vec<_>>();
        for peer in stale {
            remove_exclusion(self.ipv6, self.egress.name(), self.gateway.as_deref(), peer)?;
            self.journal.forget_exclusion(peer)?;
        }
        Ok(())
    }

    fn install_exclusions(&mut self, excluded: &[IpAddr]) -> io::Result<()> {
        for peer in excluded.iter().copied() {
            self.install_exclusion(peer)?;
        }
        Ok(())
    }

    fn remove_owned_exclusions(&mut self) -> io::Result<()> {
        for peer in self.journal.excluded.clone().into_iter().rev() {
            remove_exclusion(self.ipv6, self.egress.name(), self.gateway.as_deref(), peer)?;
            self.journal.forget_exclusion(peer)?;
        }
        Ok(())
    }

    fn cleanup(&mut self) -> io::Result<()> {
        let ipv6 = self.ipv6;
        let tun_name = self.tun_name.clone();
        let egress_name = self.egress.name().to_owned();
        let gateway = self.gateway.clone();
        self.journal.cleanup(
            |prefix| remove_route(ipv6, &tun_name, prefix),
            |peer| remove_exclusion(ipv6, &egress_name, gateway.as_deref(), peer),
        )
    }
}

fn select_physical_egress(
    ipv6: bool,
    tun_name: &str,
    allow_cross_family_carrier: bool,
) -> io::Result<LinuxEgressSelection> {
    match default_interface(ipv6, tun_name) {
        Ok((carrier, gateway)) => Ok(LinuxEgressSelection {
            family: FamilyEgressState::Available(carrier.clone()),
            carrier,
            gateway,
        }),
        Err(error) if allow_cross_family_carrier => {
            let (carrier, gateway) = default_interface(!ipv6, tun_name).map_err(|fallback| {
                io::Error::new(
                    fallback.kind(),
                    format!(
                        "default route unavailable for the TUN address family ({error}); fallback family also unavailable ({fallback})"
                    ),
                )
            })?;
            Ok(LinuxEgressSelection {
                carrier,
                gateway,
                family: FamilyEgressState::Unavailable(EgressUnavailableReason::NoDefaultRoute),
            })
        }
        Err(error) => Err(error),
    }
}

impl RouteReconcileState for SystemRouteGuard {
    type Gateway = Option<String>;

    fn current_egress(&self) -> &RouteInterface {
        &self.egress
    }

    fn current_gateway(&self) -> &Self::Gateway {
        &self.gateway
    }

    fn current_exclusions(&self) -> &[IpAddr] {
        &self.excluded
    }

    fn owned_exclusions(&self) -> Vec<IpAddr> {
        self.journal.excluded.clone()
    }

    fn reconcile_exclusions(&mut self, desired: &[IpAddr]) -> io::Result<()> {
        SystemRouteGuard::reconcile_exclusions(self, desired)
    }

    fn remove_owned_exclusions(&mut self) -> io::Result<()> {
        SystemRouteGuard::remove_owned_exclusions(self)
    }

    fn replace_egress(&mut self, egress: RouteInterface, gateway: Self::Gateway) -> io::Result<()> {
        self.journal
            .replace_egress(egress.clone(), gateway.clone())?;
        self.egress = egress;
        self.gateway = gateway;
        Ok(())
    }

    fn install_exclusions(&mut self, excluded: &[IpAddr]) -> io::Result<()> {
        SystemRouteGuard::install_exclusions(self, excluded)
    }

    fn set_current_exclusions(&mut self, excluded: Vec<IpAddr>) {
        self.excluded = excluded;
    }
}

fn add_exclusion(
    ipv6: bool,
    egress: &RouteInterface,
    gateway: Option<&str>,
    peer: IpAddr,
) -> io::Result<()> {
    let mut arguments = family_arguments(ipv6);
    arguments.extend(["route".to_owned(), "add".to_owned(), host_prefix(peer)]);
    if let Some(gateway) = gateway {
        arguments.extend(["via".to_owned(), gateway.to_owned()]);
    }
    arguments.extend(["dev".to_owned(), egress.name().to_owned()]);
    run_ip(&arguments).map(|_| ())
}

fn recover_stale_routes(lease: &RouteLease, ipv6: bool) -> io::Result<()> {
    let Some(mut journal) = RouteJournal::load(lease, ipv6)? else {
        return Ok(());
    };
    let stale_tun = journal.tun_name.clone();
    let stale_egress = journal.egress.name().to_owned();
    let stale_gateway = journal.gateway.clone();
    journal.cleanup(
        |prefix| remove_route(ipv6, &stale_tun, prefix),
        |peer| remove_exclusion(ipv6, &stale_egress, stale_gateway.as_deref(), peer),
    )
}

fn remove_route(ipv6: bool, tun_name: &str, prefix: &str) -> io::Result<()> {
    let mut arguments = family_arguments(ipv6);
    arguments.extend([
        "route".to_owned(),
        "del".to_owned(),
        prefix.to_owned(),
        "dev".to_owned(),
        tun_name.to_owned(),
    ]);
    run_ip_remove(&arguments)
}

fn remove_exclusion(
    ipv6: bool,
    egress_name: &str,
    gateway: Option<&str>,
    peer: IpAddr,
) -> io::Result<()> {
    let mut arguments = family_arguments(ipv6);
    arguments.extend(["route".to_owned(), "del".to_owned(), host_prefix(peer)]);
    if let Some(gateway) = gateway {
        arguments.extend(["via".to_owned(), gateway.to_owned()]);
    }
    arguments.extend(["dev".to_owned(), egress_name.to_owned()]);
    run_ip_remove(&arguments)
}

impl Drop for SystemRouteGuard {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

fn default_interface(ipv6: bool, tun_name: &str) -> io::Result<(RouteInterface, Option<String>)> {
    let mut arguments = family_arguments(ipv6);
    arguments.extend(["route".to_owned(), "show".to_owned(), "default".to_owned()]);
    let output = run_ip(&arguments)?;
    let route = parse_default_routes(&output)?
        .into_iter()
        .filter(|route| route.dev != tun_name)
        .min_by_key(|route| route.metric)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "default route not found"))?;
    let name = route.dev;
    let name_c = std::ffi::CString::new(name.as_str()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "interface name contains a nul byte",
        )
    })?;
    let index = unsafe { libc::if_nametoindex(name_c.as_ptr()) };
    RouteInterface::new(name, index).map(|interface| (interface, route.gateway))
}

fn parse_default_routes(output: &[u8]) -> io::Result<Vec<LinuxRoute>> {
    let output = std::str::from_utf8(output).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Linux default route output is not UTF-8: {error}"),
        )
    })?;
    output
        .lines()
        .filter_map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            (fields.first() == Some(&"default")).then_some(fields)
        })
        .map(|fields| {
            let value_after = |name: &str| {
                fields
                    .iter()
                    .position(|field| *field == name)
                    .and_then(|index| fields.get(index + 1))
                    .copied()
            };
            let dev = value_after("dev").ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Linux default route has no interface: {}", fields.join(" ")),
                )
            })?;
            let metric = value_after("metric")
                .map(str::parse::<u32>)
                .transpose()
                .map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("parse Linux default route metric: {error}"),
                    )
                })?
                .unwrap_or_default();
            Ok(LinuxRoute {
                dev: dev.to_owned(),
                gateway: value_after("via").map(str::to_owned),
                metric,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests;

fn family_arguments(ipv6: bool) -> Vec<String> {
    if ipv6 {
        vec!["-6".to_owned()]
    } else {
        vec!["-4".to_owned()]
    }
}

fn run_ip(arguments: &[String]) -> io::Result<Vec<u8>> {
    let output = Command::new("ip")
        .args(arguments)
        .output()
        .map_err(|error| io::Error::new(error.kind(), format!("execute `ip`: {error}")))?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(command_error("ip", arguments, &output.stderr))
    }
}

fn run_ip_remove(arguments: &[String]) -> io::Result<()> {
    let output = Command::new("ip")
        .args(arguments)
        .output()
        .map_err(|error| io::Error::new(error.kind(), format!("execute `ip`: {error}")))?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    if output.status.success()
        || stderr.contains("No such process")
        || stderr.contains("Cannot find device")
        || stderr.contains("No such file")
    {
        Ok(())
    } else {
        Err(command_error("ip", arguments, &output.stderr))
    }
}
