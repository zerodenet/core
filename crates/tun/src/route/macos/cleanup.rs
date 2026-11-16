//! Retire only the route described by the original journal, never a replacement
//! installed by another VPN or an administrator.
use super::{audit::table, family, run_route_remove, RouteInterface};
use ipnet::IpNet;
use std::{ffi::CString, io, net::IpAddr};

pub(super) fn remove_exclusion(
    ipv6: bool,
    egress: &RouteInterface,
    gateway: Option<&str>,
    peer: IpAddr,
) -> io::Result<()> {
    let prefix = IpNet::new(peer, if ipv6 { 128 } else { 32 }).map_err(io::Error::other)?;
    remove_with(
        prefix,
        egress.name(),
        gateway,
        false,
        || table::read(ipv6),
        || {
            require_index(
                egress.name(),
                egress.index(),
                interface_index(egress.name())?,
            )?;
            let mut args = vec![
                "-n".into(),
                "delete".into(),
                family(ipv6).into(),
                "-host".into(),
                peer.to_string(),
            ];
            if let Some(gateway) = gateway {
                args.push(gateway.into());
            } else {
                args.extend(["-interface".into(), egress.name().into()]);
            }
            args.extend(["-ifp".into(), egress.name().into()]);
            run_route_remove(&args)
        },
    )
}

pub(super) fn remove_capture(
    ipv6: bool,
    tun: &str,
    index: u32,
    capture_gateway: Option<&str>,
    prefix: &str,
) -> io::Result<()> {
    let parsed: IpNet = prefix.parse().map_err(io::Error::other)?;
    // A repaired point-to-point peer is an interface route, not a route via itself.
    let gateway = capture_gateway.filter(|peer| parsed.to_string() != format!("{peer}/32"));
    remove_with(
        parsed,
        tun,
        gateway,
        false,
        || table::read(ipv6),
        || {
            require_index(tun, index, interface_index(tun)?)?;
            let mut args = super::route_remove_arguments(ipv6, prefix);
            args.extend(["-ifp".into(), tun.into()]);
            run_route_remove(&args)
        },
    )
}

pub(super) fn remove_scoped_bypass(
    ipv6: bool,
    egress: &RouteInterface,
    gateway: Option<&str>,
) -> io::Result<()> {
    remove_with(
        if ipv6 { "::/0" } else { "0.0.0.0/0" }.parse().unwrap(),
        egress.name(),
        gateway,
        true,
        || table::read(ipv6),
        || {
            require_index(
                egress.name(),
                egress.index(),
                interface_index(egress.name())?,
            )?;
            super::scoped::remove_scoped_bypass(ipv6, egress.name())
        },
    )
}

pub(super) fn interface_index(name: &str) -> io::Result<u32> {
    let name = CString::new(name).map_err(io::Error::other)?;
    // SAFETY: the NUL-terminated interface name lives through this lookup.
    let index = unsafe { libc::if_nametoindex(name.as_ptr()) };
    if index == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(index)
    }
}

fn require_index(name: &str, expected: u32, observed: u32) -> io::Result<()> {
    if expected == 0 || expected != observed {
        return Err(io::Error::other(format!("cannot confirm original TUN route interface {name}: recorded index {expected}, current index {observed}")));
    }
    Ok(())
}

fn remove_with(
    prefix: IpNet,
    interface: &str,
    gateway: Option<&str>,
    scoped: bool,
    mut read: impl FnMut() -> io::Result<Vec<table::RouteEntry>>,
    remove: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    let matching = |row: &&table::RouteEntry| {
        row.prefix == prefix
            && row.flags.contains('I') == scoped
            && !row.flags.contains('L')
            && (!scoped || row.interface == interface)
    };
    let same_gateway = |row: &table::RouteEntry| {
        gateway.map_or_else(
            || !row.flags.contains('G'),
            |gateway| row.gateway == gateway,
        )
    };
    let before = read()?;
    let rows: Vec<_> = before.iter().filter(matching).collect();
    if rows.is_empty() {
        return Ok(());
    }
    if rows
        .iter()
        .any(|row| row.interface != interface || !same_gateway(row))
    {
        return Err(io::Error::other(format!("refusing to remove replaced TUN route {prefix}: original interface {interface}, gateway {gateway:?}")));
    }
    remove()?;
    if read()?
        .iter()
        .filter(matching)
        .any(|row| row.interface == interface && same_gateway(row))
    {
        return Err(io::Error::other(format!(
            "TUN route removal not confirmed for {prefix} on {interface}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
