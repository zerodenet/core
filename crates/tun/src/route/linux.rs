use std::io;
use std::net::IpAddr;
use std::process::Command;

use serde::Deserialize;

use super::{
    command_error, host_prefix, split_default_route_prefixes, RouteInterface, RouteJournal,
    RouteLease,
};

#[derive(Debug)]
pub struct SystemRouteGuard {
    egress: RouteInterface,
    tun_name: String,
    ipv6: bool,
    gateway: Option<String>,
    journal: RouteJournal,
}

#[derive(Deserialize)]
struct LinuxRoute {
    dev: Option<String>,
    gateway: Option<String>,
}

impl SystemRouteGuard {
    pub fn install(
        tun_name: &str,
        recovery_key: &str,
        address: IpAddr,
        excluded: &[IpAddr],
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
        let journal = RouteJournal::new(lease, tun_name, ipv6, 0, egress.clone())?;
        let mut guard = Self {
            egress,
            tun_name: tun_name.to_owned(),
            ipv6,
            gateway,
            journal,
        };
        for peer in excluded
            .iter()
            .copied()
            .filter(|peer| peer.is_ipv6() == ipv6)
        {
            guard.add_exclusion(peer)?;
            guard.journal.record_exclusion(peer)?;
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

    fn add_exclusion(&self, peer: IpAddr) -> io::Result<()> {
        let mut arguments = family_arguments(self.ipv6);
        arguments.extend(["route".to_owned(), "add".to_owned(), host_prefix(peer)]);
        if let Some(gateway) = &self.gateway {
            arguments.extend(["via".to_owned(), gateway.clone()]);
        }
        arguments.extend(["dev".to_owned(), self.egress.name().to_owned()]);
        run_ip(&arguments).map(|_| ())
    }

    fn cleanup(&mut self) -> io::Result<()> {
        let ipv6 = self.ipv6;
        let tun_name = self.tun_name.clone();
        let egress_name = self.egress.name().to_owned();
        self.journal.cleanup(
            |prefix| remove_route(ipv6, &tun_name, prefix),
            |peer| remove_exclusion(ipv6, &egress_name, peer),
        )
    }
}

fn recover_stale_routes(lease: &RouteLease, ipv6: bool) -> io::Result<()> {
    let Some(mut journal) = RouteJournal::load(lease, ipv6)? else {
        return Ok(());
    };
    let stale_tun = journal.tun_name.clone();
    let stale_egress = journal.egress.name().to_owned();
    journal.cleanup(
        |prefix| remove_route(ipv6, &stale_tun, prefix),
        |peer| remove_exclusion(ipv6, &stale_egress, peer),
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

fn remove_exclusion(ipv6: bool, egress_name: &str, peer: IpAddr) -> io::Result<()> {
    let mut arguments = family_arguments(ipv6);
    arguments.extend([
        "route".to_owned(),
        "del".to_owned(),
        host_prefix(peer),
        "dev".to_owned(),
        egress_name.to_owned(),
    ]);
    run_ip_remove(&arguments)
}

impl Drop for SystemRouteGuard {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

fn default_interface(ipv6: bool, tun_name: &str) -> io::Result<(RouteInterface, Option<String>)> {
    let mut arguments = family_arguments(ipv6);
    arguments.extend([
        "-json".to_owned(),
        "route".to_owned(),
        "show".to_owned(),
        "default".to_owned(),
    ]);
    let output = run_ip(&arguments)?;
    let routes: Vec<LinuxRoute> = serde_json::from_slice(&output).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("parse Linux default route: {error}"),
        )
    })?;
    let route = routes
        .into_iter()
        .find(|route| route.dev.as_deref().is_some_and(|name| name != tun_name))
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "default route not found"))?;
    let name = route.dev.expect("filtered route has an interface");
    let name_c = std::ffi::CString::new(name.as_str()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "interface name contains a nul byte",
        )
    })?;
    let index = unsafe { libc::if_nametoindex(name_c.as_ptr()) };
    RouteInterface::new(name, index).map(|interface| (interface, route.gateway))
}

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
