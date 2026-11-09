//! Metadata-only commands run by the same already registered route worker.
//! They never go through or enlarge the privileged helper command whitelist.
use super::*;
use std::{net::IpAddr, process::Output};
impl MacosHelperAccess {
    pub fn system_dns_servers(&self) -> io::Result<Vec<IpAddr>> {
        let owner = MacosHelperOwner(
            self.0
                .upgrade()
                .ok_or_else(|| io::Error::other("actual helper owner expired"))?,
        );
        let output = owner.query(tool::Query::SystemDns)?;
        if output.status.success() {
            let servers = parse_dns(&output.stdout);
            if !servers.is_empty() {
                return Ok(servers);
            }
        }
        // A successfully retired query with no usable result preserves the old
        // resolv.conf fallback. Native close/Child errors are never None.
        let bytes = owner.resolv_conf()?;
        Ok(parse_dns(&bytes))
    }
}
impl MacosHelperOwner {
    pub(in crate::macos_privilege) fn query(&self, query: tool::Query) -> io::Result<Output> {
        let index = self.0.current.load(Ordering::Acquire);
        let mut epoch = self.0.epochs[index]
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        if epoch
            .local_file
            .as_ref()
            .is_some_and(|original| !original.closed())
        {
            return Err(io::Error::other("prior local metadata FD remains unknown"));
        }
        let result = tool::run_query(&mut epoch.local_tool, query, self.0.output_limit);
        if let Err(source) = result {
            self.0.retain(index, Phase::ToolProgress, source);
            return Err(io::Error::other(self.0.failures.clone().into_error()));
        }
        result
    }
    fn resolv_conf(&self) -> io::Result<Vec<u8>> {
        let index = self.0.current.load(Ordering::Acquire);
        let mut epoch = self.0.epochs[index]
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        if epoch
            .local_file
            .as_ref()
            .is_some_and(|original| !original.closed())
        {
            return Err(io::Error::other(
                "same local metadata file has no close receipt",
            ));
        }
        let fd = unsafe {
            libc::open(
                c"/etc/resolv.conf".as_ptr(),
                libc::O_RDONLY | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        epoch.local_file = Some(Original::new(fd, epoch.id)); // before the first read
        let result = (|| {
            let mut output = Vec::new();
            loop {
                let mut bytes = [0u8; 4096];
                let count = unsafe { libc::read(fd, bytes.as_mut_ptr().cast(), bytes.len()) };
                if count == 0 {
                    return Ok(output);
                }
                if count < 0 {
                    let source = io::Error::last_os_error();
                    if source.kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(source);
                }
                if output
                    .len()
                    .checked_add(count as usize)
                    .is_none_or(|len| len > self.0.output_limit)
                {
                    return Err(io::Error::other(
                        "system DNS file exceeds actual file-read allowance",
                    ));
                }
                output.extend_from_slice(&bytes[..count as usize]);
            }
        })();
        // Always attempt the one original close, even if reading failed.
        let close = epoch.local_file.as_mut().unwrap().close_once();
        if let Err(source) = close {
            self.0.retain(index, Phase::ToolClose, source);
            return Err(io::Error::other(self.0.failures.clone().into_error()));
        }
        result
    }
}
fn parse_dns(bytes: &[u8]) -> Vec<IpAddr> {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            let value = if let Some(value) = line.strip_prefix("nameserver ") {
                value.trim()
            } else if line.starts_with("nameserver[") {
                line.split_once(':')?.1.trim()
            } else {
                return None;
            };
            value
                .split_whitespace()
                .next()?
                .split('%')
                .next()?
                .parse()
                .ok()
        })
        .collect()
}
/// None is the explicit independent-library path, not a Kernel fallback after
/// failure. A present actual Source context always returns Some, including Err.
pub(crate) fn route_query(arguments: &[String]) -> Option<io::Result<Output>> {
    current().map(|owner| owner.query(tool::Query::Route(arguments.to_vec())))
}
pub(crate) fn route_table(ipv6: bool) -> Option<io::Result<Output>> {
    current().map(|owner| owner.query(tool::Query::RouteTable(ipv6)))
}
