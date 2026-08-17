use std::io;
use std::process::Command;

use super::{family, route_program, run_route, run_route_remove};
use crate::route::{command_error, RouteInterface};

pub(super) fn gateway_matches_family(ipv6: bool, gateway: Option<&str>) -> bool {
    match gateway {
        None => true,
        Some(gateway) if ipv6 => gateway.contains(':'),
        Some(gateway) => gateway.parse::<std::net::Ipv4Addr>().is_ok(),
    }
}

pub(super) fn ensure_scoped_bypass(
    ipv6: bool,
    egress: &RouteInterface,
    gateway: Option<&str>,
) -> io::Result<bool> {
    if scoped_bypass_exists(ipv6, egress.name())? {
        return Ok(false);
    }
    run_route(&scoped_bypass_add_arguments(ipv6, egress.name(), gateway))?;
    Ok(true)
}

fn scoped_bypass_exists(ipv6: bool, egress_name: &str) -> io::Result<bool> {
    let arguments = scoped_bypass_get_arguments(ipv6, egress_name);
    let program = route_program();
    let output = Command::new(program)
        .args(&arguments)
        .output()
        .map_err(|error| io::Error::new(error.kind(), format!("execute `{program}`: {error}")))?;
    if output.status.success() {
        return Ok(route_output_has_scoped_flag(&output.stdout));
    }
    let stderr = String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();
    if stderr.contains("not in table")
        || stderr.contains("not found")
        || stderr.contains("no such process")
    {
        Ok(false)
    } else {
        Err(command_error(program, &arguments, &output.stderr))
    }
}

pub(super) fn route_output_has_scoped_flag(output: &[u8]) -> bool {
    String::from_utf8_lossy(output).lines().any(|line| {
        line.trim()
            .strip_prefix("flags:")
            .is_some_and(|flags| flags.contains("IFSCOPE"))
    })
}

pub(super) fn remove_scoped_bypass(ipv6: bool, egress_name: &str) -> io::Result<()> {
    run_route_remove(&scoped_bypass_remove_arguments(ipv6, egress_name))
}

pub(super) fn scoped_bypass_get_arguments(ipv6: bool, egress_name: &str) -> Vec<String> {
    vec![
        "-n".to_owned(),
        "get".to_owned(),
        family(ipv6).to_owned(),
        "-ifscope".to_owned(),
        egress_name.to_owned(),
        "default".to_owned(),
    ]
}

pub(super) fn scoped_bypass_add_arguments(
    ipv6: bool,
    egress_name: &str,
    gateway: Option<&str>,
) -> Vec<String> {
    let mut arguments = vec![
        "-n".to_owned(),
        "add".to_owned(),
        family(ipv6).to_owned(),
        "-ifscope".to_owned(),
        egress_name.to_owned(),
        "default".to_owned(),
    ];
    if let Some(gateway) = gateway {
        arguments.push(gateway.to_owned());
    } else {
        arguments.extend(["-interface".to_owned(), egress_name.to_owned()]);
    }
    arguments
}

pub(super) fn scoped_bypass_remove_arguments(ipv6: bool, egress_name: &str) -> Vec<String> {
    vec![
        "-n".to_owned(),
        "delete".to_owned(),
        family(ipv6).to_owned(),
        "-ifscope".to_owned(),
        egress_name.to_owned(),
        "default".to_owned(),
    ]
}

pub(super) fn combine_cleanup_errors(
    first: io::Result<()>,
    second: io::Result<()>,
) -> io::Result<()> {
    match (first, second) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(first), Err(second)) => Err(io::Error::new(
            first.kind(),
            format!("{first}; additional cleanup failure: {second}"),
        )),
    }
}
