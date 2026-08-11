use std::io;
use std::net::IpAddr;
use std::process::Command;

use super::{
    command_error, split_default_route_prefixes, RouteInterface, RouteJournal, RouteLease,
};

#[derive(Debug)]
pub struct SystemRouteGuard {
    egress: RouteInterface,
    tun_name: String,
    ipv6: bool,
    gateway: Option<String>,
    journal: RouteJournal,
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
        run_route(&[
            "-n".to_owned(),
            "add".to_owned(),
            family(self.ipv6).to_owned(),
            prefix.to_owned(),
            "-interface".to_owned(),
            self.tun_name.clone(),
        ])
        .map(|_| ())
    }

    fn remove(&self, prefix: &str) -> io::Result<()> {
        remove_route(self.ipv6, &self.tun_name, prefix)
    }

    fn add_exclusion(&self, peer: IpAddr) -> io::Result<()> {
        let mut arguments = vec![
            "-n".to_owned(),
            "add".to_owned(),
            family(self.ipv6).to_owned(),
            "-host".to_owned(),
            peer.to_string(),
        ];
        if let Some(gateway) = &self.gateway {
            arguments.push(gateway.clone());
        } else {
            arguments.extend(["-interface".to_owned(), self.egress.name().to_owned()]);
        }
        run_route(&arguments).map(|_| ())
    }

    fn cleanup(&mut self) -> io::Result<()> {
        let ipv6 = self.ipv6;
        let tun_name = self.tun_name.clone();
        self.journal.cleanup(
            |prefix| remove_route(ipv6, &tun_name, prefix),
            |peer| remove_exclusion(ipv6, peer),
        )
    }
}

fn recover_stale_routes(lease: &RouteLease, ipv6: bool) -> io::Result<()> {
    let Some(mut journal) = RouteJournal::load(lease, ipv6)? else {
        return Ok(());
    };
    let stale_tun = journal.tun_name.clone();
    journal.cleanup(
        |prefix| remove_route(ipv6, &stale_tun, prefix),
        |peer| remove_exclusion(ipv6, peer),
    )
}

fn remove_route(ipv6: bool, tun_name: &str, prefix: &str) -> io::Result<()> {
    run_route_remove(&[
        "-n".to_owned(),
        "delete".to_owned(),
        family(ipv6).to_owned(),
        prefix.to_owned(),
        "-interface".to_owned(),
        tun_name.to_owned(),
    ])
}

fn remove_exclusion(ipv6: bool, peer: IpAddr) -> io::Result<()> {
    run_route_remove(&[
        "-n".to_owned(),
        "delete".to_owned(),
        family(ipv6).to_owned(),
        "-host".to_owned(),
        peer.to_string(),
    ])
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
    let output = String::from_utf8_lossy(&output);
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
    let name_c = std::ffi::CString::new(name.as_str()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "interface name contains a nul byte",
        )
    })?;
    let index = unsafe { libc::if_nametoindex(name_c.as_ptr()) };
    RouteInterface::new(name, index).map(|interface| (interface, gateway))
}

fn family(ipv6: bool) -> &'static str {
    if ipv6 {
        "-inet6"
    } else {
        "-inet"
    }
}

fn run_route(arguments: &[String]) -> io::Result<Vec<u8>> {
    let program = if std::path::Path::new("/sbin/route").exists() {
        "/sbin/route"
    } else {
        "route"
    };
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
    let program = if std::path::Path::new("/sbin/route").exists() {
        "/sbin/route"
    } else {
        "route"
    };
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
