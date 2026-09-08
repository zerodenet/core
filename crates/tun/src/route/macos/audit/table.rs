use std::io;
use std::net::{IpAddr, Ipv4Addr};
use std::process::Command;

use ipnet::IpNet;

#[derive(Debug, Clone)]
pub(super) struct RouteEntry {
    pub prefix: IpNet,
    pub gateway: String,
    pub interface: String,
    pub flags: String,
}

pub(super) fn read(ipv6: bool) -> io::Result<Vec<RouteEntry>> {
    let output = Command::new("/usr/sbin/netstat")
        .args(["-rnW", "-f", if ipv6 { "inet6" } else { "inet" }])
        .env("LC_ALL", "C")
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "read macOS route table: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    parse(&output.stdout, ipv6)
}

pub(super) fn parse(output: &[u8], ipv6: bool) -> io::Result<Vec<RouteEntry>> {
    let text = std::str::from_utf8(output).map_err(io::Error::other)?;
    let mut header = false;
    let mut entries = Vec::new();
    for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
        if line.starts_with("Destination") {
            header =
                line.split_whitespace()
                    .take(4)
                    .eq(["Destination", "Gateway", "Flags", "Netif"]);
            if !header {
                return Err(io::Error::other("unrecognized macOS route table columns"));
            }
            continue;
        }
        if !header {
            continue;
        }
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() < 4 {
            return Err(io::Error::other(format!("invalid macOS route row: {line}")));
        }
        entries.push(RouteEntry {
            prefix: parse_prefix(fields[0], ipv6)?,
            gateway: fields[1].into(),
            flags: fields[2].into(),
            interface: fields[3].into(),
        });
    }
    if !header {
        return Err(io::Error::other("macOS route table header missing"));
    }
    Ok(entries)
}

fn parse_prefix(value: &str, ipv6: bool) -> io::Result<IpNet> {
    if value == "default" {
        return Ok(if ipv6 { "::/0" } else { "0.0.0.0/0" }.parse().unwrap());
    }
    let (address, length) = value
        .split_once('/')
        .map_or((value, None), |(address, length)| (address, Some(length)));
    // netstat annotates scoped IPv6 addresses with the interface name.
    let address = address.split('%').next().unwrap();
    let (address, inferred_length): (IpAddr, u8) = if ipv6 {
        (address.parse().map_err(io::Error::other)?, 128)
    } else {
        // BSD abbreviates network destinations, e.g. 128/1 and 192.168.0.
        let fields: Vec<_> = address.split('.').collect();
        if fields.is_empty() || fields.len() > 4 {
            return Err(io::Error::other("invalid IPv4 route destination"));
        }
        let mut octets = [0; 4];
        for (octet, field) in octets.iter_mut().zip(&fields) {
            *octet = field.parse().map_err(io::Error::other)?;
        }
        (Ipv4Addr::from(octets).into(), (fields.len() * 8) as u8)
    };
    if address.is_ipv6() != ipv6 {
        return Err(io::Error::other("route table address family mismatch"));
    }
    let length = length
        .map(str::parse)
        .transpose()
        .map_err(io::Error::other)?
        .unwrap_or(inferred_length);
    IpNet::new(address, length)
        .map(|prefix| prefix.trunc())
        .map_err(io::Error::other)
}
