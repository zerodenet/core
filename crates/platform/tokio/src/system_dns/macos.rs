use std::io;
use std::net::IpAddr;
use std::process::Command;

pub(super) fn system_dns_servers() -> io::Result<Vec<IpAddr>> {
    let output = Command::new("scutil").arg("--dns").output();
    if let Ok(output) = output {
        if output.status.success() {
            let servers = super::parse_nameserver_lines(&String::from_utf8_lossy(&output.stdout));
            if !servers.is_empty() {
                return Ok(servers);
            }
        }
    }

    let contents = std::fs::read_to_string("/etc/resolv.conf").map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("discover macOS system DNS with scutil or /etc/resolv.conf: {error}"),
        )
    })?;
    Ok(super::parse_nameserver_lines(&contents))
}
