use std::io;
use std::net::IpAddr;
use std::path::Path;

const RESOLVER_PATHS: [&str; 3] = [
    "/run/systemd/resolve/resolv.conf",
    "/run/NetworkManager/no-stub-resolv.conf",
    "/etc/resolv.conf",
];

pub(super) fn system_dns_servers() -> io::Result<Vec<IpAddr>> {
    let mut servers = Vec::new();
    let mut readable = false;
    let mut last_error = None;
    for path in RESOLVER_PATHS {
        if !Path::new(path).exists() {
            continue;
        }
        match std::fs::read_to_string(path) {
            Ok(contents) => {
                readable = true;
                servers.extend(super::parse_nameserver_lines(&contents));
            }
            Err(error) => last_error = Some(error),
        }
    }
    if readable {
        Ok(servers)
    } else {
        Err(last_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "no readable system resolver configuration was found",
            )
        }))
    }
}
