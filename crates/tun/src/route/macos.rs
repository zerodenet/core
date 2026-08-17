use std::io;
use std::net::IpAddr;
use std::process::Command;

use super::reconcile::{reconcile_route_state, with_rollback_error, RouteReconcileState};
use super::{
    command_error, family_exclusions, split_default_route_prefixes, RouteInterface, RouteJournal,
    RouteLease,
};

mod scoped;

use scoped::{
    combine_cleanup_errors, ensure_scoped_bypass, gateway_matches_family, remove_scoped_bypass,
};

#[derive(Debug)]
pub struct SystemRouteGuard {
    egress: RouteInterface,
    tun_name: String,
    ipv6: bool,
    gateway: Option<String>,
    tun_gateway: Option<String>,
    excluded: Vec<IpAddr>,
    journal: RouteJournal,
}

impl SystemRouteGuard {
    pub fn install(
        tun_name: &str,
        recovery_key: &str,
        address: IpAddr,
        netmask: IpAddr,
        excluded: &[IpAddr],
    ) -> io::Result<Self> {
        Self::install_with_egress(tun_name, recovery_key, address, netmask, excluded, |_| {
            Ok(())
        })
    }

    pub fn install_with_egress(
        tun_name: &str,
        recovery_key: &str,
        address: IpAddr,
        netmask: IpAddr,
        excluded: &[IpAddr],
        publish_egress: impl FnOnce(&RouteInterface) -> io::Result<()>,
    ) -> io::Result<Self> {
        let ipv6 = address.is_ipv6();
        let lease = RouteLease::acquire(recovery_key, ipv6)?;
        recover_stale_routes(&lease, ipv6)?;
        let has_family_exclusions = excluded.iter().any(|peer| peer.is_ipv6() == ipv6);
        let (egress, gateway) = default_interface(ipv6, tun_name).or_else(|error| {
            if has_family_exclusions {
                Err(error)
            } else {
                default_interface(!ipv6, tun_name).map_err(|fallback| {
                    io::Error::new(
                        fallback.kind(),
                        format!(
                            "default route unavailable for the TUN address family ({error}); fallback family also unavailable ({fallback})"
                        ),
                    )
                })
            }
        })?;
        let tun_gateway = match address {
            IpAddr::V4(address) => Some(crate::macos::ipv4_peer(address, netmask)?.to_string()),
            IpAddr::V6(_) => None,
        };
        let journal = RouteJournal::new(lease, tun_name, ipv6, 0, egress.clone(), gateway.clone())?;
        let desired_exclusions = family_exclusions(excluded, ipv6);
        let mut guard = Self {
            egress,
            tun_name: tun_name.to_owned(),
            ipv6,
            gateway,
            tun_gateway,
            excluded: desired_exclusions.clone(),
            journal,
        };
        publish_egress(&guard.egress)?;
        guard.install_scoped_bypass()?;
        for peer in desired_exclusions {
            guard.install_exclusion(peer)?;
        }
        for prefix in split_default_route_prefixes(address) {
            let _ = guard.remove(prefix);
            guard.add(prefix)?;
            guard.journal.record_route(prefix)?;
        }
        Ok(guard)
    }

    pub fn egress(&self) -> &RouteInterface {
        &self.egress
    }

    pub fn is_ipv6(&self) -> bool {
        self.ipv6
    }

    /// Re-resolve the preferred physical interface and reconcile explicit
    /// bypass routes without replacing the TUN device or split default routes.
    pub fn reconcile(&mut self, excluded: &[IpAddr]) -> io::Result<bool> {
        let desired_exclusions = family_exclusions(excluded, self.ipv6);
        let has_family_exclusions = !desired_exclusions.is_empty();
        let (desired_egress, desired_gateway) =
            default_interface(self.ipv6, &self.tun_name).or_else(|error| {
                if has_family_exclusions {
                    Err(error)
                } else {
                    default_interface(!self.ipv6, &self.tun_name).map_err(|fallback| {
                        io::Error::new(
                            fallback.kind(),
                            format!(
                                "default route unavailable for the TUN address family ({error}); fallback family also unavailable ({fallback})"
                            ),
                        )
                    })
                }
            })?;
        reconcile_route_state(self, desired_egress, desired_gateway, desired_exclusions)
    }

    pub fn close(mut self) -> io::Result<()> {
        self.cleanup()
    }

    fn add(&self, prefix: &str) -> io::Result<()> {
        let arguments = route_add_arguments(
            self.ipv6,
            &self.tun_name,
            self.tun_gateway.as_deref(),
            prefix,
        );
        run_route(&arguments).map(|_| ())
    }

    fn remove(&self, prefix: &str) -> io::Result<()> {
        remove_route(self.ipv6, prefix)
    }

    fn install_exclusion(&mut self, peer: IpAddr) -> io::Result<()> {
        self.journal.record_exclusion(peer)?;
        if let Err(error) = add_exclusion(&self.egress, self.gateway.as_deref(), self.ipv6, peer) {
            let rollback = self.journal.forget_exclusion(peer);
            return Err(with_rollback_error(error, rollback));
        }
        Ok(())
    }

    fn install_scoped_bypass(&mut self) -> io::Result<()> {
        if self.journal.scoped_bypass || !gateway_matches_family(self.ipv6, self.gateway.as_deref())
        {
            return Ok(());
        }
        if !ensure_scoped_bypass(self.ipv6, &self.egress, self.gateway.as_deref())? {
            return Ok(());
        }
        if let Err(error) = self.journal.record_scoped_bypass() {
            let rollback = remove_scoped_bypass(self.ipv6, self.egress.name());
            return Err(with_rollback_error(error, rollback));
        }
        Ok(())
    }

    fn remove_owned_scoped_bypass(&mut self) -> io::Result<()> {
        if !self.journal.scoped_bypass {
            return Ok(());
        }
        remove_scoped_bypass(self.ipv6, self.egress.name())?;
        self.journal.forget_scoped_bypass()
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
            remove_exclusion(self.ipv6, self.gateway.as_deref(), peer)?;
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
            remove_exclusion(self.ipv6, self.gateway.as_deref(), peer)?;
            self.journal.forget_exclusion(peer)?;
        }
        Ok(())
    }

    fn cleanup(&mut self) -> io::Result<()> {
        let ipv6 = self.ipv6;
        let gateway = self.gateway.clone();
        let bypass = self.remove_owned_scoped_bypass();
        let routes = self.journal.cleanup(
            |prefix| remove_route(ipv6, prefix),
            |peer| remove_exclusion(ipv6, gateway.as_deref(), peer),
        );
        combine_cleanup_errors(bypass, routes)
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
        let old_egress = self.egress.clone();
        let old_gateway = self.gateway.clone();
        self.remove_owned_scoped_bypass()?;
        if let Err(error) = self.journal.replace_egress(egress.clone(), gateway.clone()) {
            let rollback = self.install_scoped_bypass();
            return Err(with_rollback_error(error, rollback));
        }
        self.egress = egress;
        self.gateway = gateway;
        if let Err(error) = self.install_scoped_bypass() {
            let rollback = (|| {
                self.journal
                    .replace_egress(old_egress.clone(), old_gateway.clone())?;
                self.egress = old_egress;
                self.gateway = old_gateway;
                self.install_scoped_bypass()
            })();
            return Err(with_rollback_error(error, rollback));
        }
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
    egress: &RouteInterface,
    gateway: Option<&str>,
    ipv6: bool,
    peer: IpAddr,
) -> io::Result<()> {
    let mut arguments = vec![
        "-n".to_owned(),
        "add".to_owned(),
        family(ipv6).to_owned(),
        "-host".to_owned(),
        peer.to_string(),
    ];
    if let Some(gateway) = gateway {
        arguments.push(gateway.to_owned());
    } else {
        arguments.extend(["-interface".to_owned(), egress.name().to_owned()]);
    }
    run_route(&arguments).map(|_| ())
}

fn recover_stale_routes(lease: &RouteLease, ipv6: bool) -> io::Result<()> {
    let Some(mut journal) = RouteJournal::load(lease, ipv6)? else {
        return Ok(());
    };
    let bypass = if journal.scoped_bypass {
        remove_scoped_bypass(ipv6, journal.egress.name())
            .and_then(|()| journal.forget_scoped_bypass())
    } else {
        Ok(())
    };
    let stale_gateway = journal.gateway.clone();
    let routes = journal.cleanup(
        |prefix| remove_route(ipv6, prefix),
        |peer| remove_exclusion(ipv6, stale_gateway.as_deref(), peer),
    );
    combine_cleanup_errors(bypass, routes)
}

fn route_add_arguments(
    ipv6: bool,
    tun_name: &str,
    tun_gateway: Option<&str>,
    prefix: &str,
) -> Vec<String> {
    let mut arguments = vec![
        "-n".to_owned(),
        "add".to_owned(),
        family(ipv6).to_owned(),
        prefix.to_owned(),
    ];
    if let Some(gateway) = tun_gateway {
        arguments.push(gateway.to_owned());
    } else {
        arguments.extend(["-interface".to_owned(), tun_name.to_owned()]);
    }
    arguments
}

fn remove_route(ipv6: bool, prefix: &str) -> io::Result<()> {
    run_route_remove(&route_remove_arguments(ipv6, prefix))
}

fn route_remove_arguments(ipv6: bool, prefix: &str) -> Vec<String> {
    vec![
        "-n".to_owned(),
        "delete".to_owned(),
        family(ipv6).to_owned(),
        prefix.to_owned(),
    ]
}

fn remove_exclusion(ipv6: bool, gateway: Option<&str>, peer: IpAddr) -> io::Result<()> {
    let mut arguments = vec![
        "-n".to_owned(),
        "delete".to_owned(),
        family(ipv6).to_owned(),
        "-host".to_owned(),
        peer.to_string(),
    ];
    if let Some(gateway) = gateway {
        arguments.push(gateway.to_owned());
    }
    run_route_remove(&arguments)
}

impl Drop for SystemRouteGuard {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

fn default_interface(ipv6: bool, tun_name: &str) -> io::Result<(RouteInterface, Option<String>)> {
    let arguments = [
        "-n".to_owned(),
        "get".to_owned(),
        family(ipv6).to_owned(),
        "default".to_owned(),
    ];
    let output = run_route(&arguments)?;
    let (name, gateway) = parse_default_route(&output, tun_name)?;
    let name_c = std::ffi::CString::new(name.as_str()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "interface name contains a nul byte",
        )
    })?;
    let index = unsafe { libc::if_nametoindex(name_c.as_ptr()) };
    RouteInterface::new(name, index).map(|interface| (interface, gateway))
}

fn parse_default_route(output: &[u8], tun_name: &str) -> io::Result<(String, Option<String>)> {
    let output = std::str::from_utf8(output).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("macOS default route output is not UTF-8: {error}"),
        )
    })?;
    let name = output
        .lines()
        .filter_map(|line| line.trim().strip_prefix("interface:"))
        .map(str::trim)
        .find(|name| !name.is_empty() && *name != tun_name)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "default route not found"))?
        .to_owned();
    let gateway = output
        .lines()
        .filter_map(|line| line.trim().strip_prefix("gateway:"))
        .map(str::trim)
        .find(|gateway| !gateway.is_empty())
        .map(str::to_owned);
    Ok((name, gateway))
}

#[cfg(test)]
mod tests;

fn family(ipv6: bool) -> &'static str {
    if ipv6 {
        "-inet6"
    } else {
        "-inet"
    }
}

fn run_route(arguments: &[String]) -> io::Result<Vec<u8>> {
    let program = route_program();
    let output = Command::new(program)
        .args(arguments)
        .output()
        .map_err(|error| io::Error::new(error.kind(), format!("execute `{program}`: {error}")))?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(command_error(program, arguments, &output.stderr))
    }
}

fn run_route_remove(arguments: &[String]) -> io::Result<()> {
    let program = route_program();
    let output = Command::new(program)
        .args(arguments)
        .output()
        .map_err(|error| io::Error::new(error.kind(), format!("execute `{program}`: {error}")))?;
    let stderr = String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();
    if output.status.success()
        || stderr.contains("not in table")
        || stderr.contains("not found")
        || stderr.contains("no such process")
    {
        Ok(())
    } else {
        Err(command_error(program, arguments, &output.stderr))
    }
}

fn route_program() -> &'static str {
    if std::path::Path::new("/sbin/route").exists() {
        "/sbin/route"
    } else {
        "route"
    }
}
