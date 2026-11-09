//! Metadata-only commands run by the same already registered route worker.
//! They never go through or enlarge the privileged helper command whitelist.
use super::super::command::CommandOutput;
use super::*;
use std::net::IpAddr;
enum QueryFailure {
    Closed(io::Error),
    Original(io::Error),
}
impl QueryFailure {
    fn into_io(self) -> io::Error {
        match self {
            Self::Closed(source) | Self::Original(source) => source,
        }
    }
}
impl MacosHelperAccess {
    pub fn system_dns_servers(&self) -> io::Result<Vec<IpAddr>> {
        let owner = MacosHelperOwner(
            self.0
                .upgrade()
                .ok_or_else(|| io::Error::other("actual helper owner expired"))?,
        );
        match owner.query(tool::Query::SystemDns) {
            Ok(output) if output.status.success() => {
                let (observed, servers) = parse_dns(&output.stdout);
                if observed {
                    return require_servers(servers);
                }
            }
            Ok(_) | Err(QueryFailure::Closed(_)) => {}
            Err(QueryFailure::Original(source)) => return Err(source),
        }
        // A successfully retired query with no usable result preserves the old
        // resolv.conf fallback. Native close/Child errors are never None.
        let bytes = owner.resolv_conf()?;
        require_servers(parse_dns(&bytes).1)
    }
}
impl MacosHelperOwner {
    fn query(&self, query: tool::Query) -> Result<CommandOutput, QueryFailure> {
        if !self.0.io_allowed() {
            return Err(QueryFailure::Original(io::Error::from(
                io::ErrorKind::Interrupted,
            )));
        }
        let _notifications = self.0.memory.defer_notifications();
        let _progress = wake::EpochProgress(&self.0);
        let index = self.0.current.load(Ordering::Acquire);
        let mut epoch = self.0.epochs[index]
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        if epoch
            .local_file
            .as_ref()
            .is_some_and(|original| !original.closed())
        {
            return Err(QueryFailure::Original(io::Error::other(
                "prior local metadata FD remains unknown",
            )));
        }
        let storage = epoch
            .rpc
            .output_storage()
            .map_err(QueryFailure::Original)?
            .clone();
        let result = tool::run_query_owned(
            &mut epoch.local_tool,
            query,
            &self.0.memory,
            &storage,
            self.0.output_limit,
        );
        match result {
            Err(source)
                if source.get_ref().is_some_and(|source| {
                    source.is::<crate::packet_memory::PacketMemoryError>()
                }) =>
            {
                Err(QueryFailure::Original(source))
            }
            Err(source) if epoch.local_tool.reusable() => Err(QueryFailure::Closed(source)),
            Err(source) => {
                self.0.retain(index, Phase::ToolProgress, source);
                Err(QueryFailure::Original(failures::retained(
                    self.0.failures.clone().into_error(),
                )))
            }
            Ok(output) => Ok(output),
        }
    }
    fn resolv_conf(&self) -> io::Result<Vec<u8>> {
        self.read_file(std::path::Path::new("/etc/resolv.conf"))
    }
}
fn require_servers(servers: Vec<IpAddr>) -> io::Result<Vec<IpAddr>> {
    if servers.is_empty() {
        Err(io::Error::new(io::ErrorKind::NotFound,"system DNS auto-discovery found no non-local upstream endpoints; configure an explicit UDP, DoH, DoT, or DoQ backend"))
    } else {
        Ok(servers)
    }
}
fn parse_dns(bytes: &[u8]) -> (bool, Vec<IpAddr>) {
    // Same production policy as platform::system_dns: sort/dedup the usable
    // endpoints, keep the first 32, and reject local-only upstream discovery.
    // A fixed stack set avoids building an unbounded intermediate address Vec.
    let mut selected = [None; 32];
    let mut count = 0;
    let mut observed = false;
    for line in String::from_utf8_lossy(bytes).lines() {
        let line = line.split('#').next().unwrap().trim();
        let Some(rest) = line.strip_prefix("nameserver") else {
            continue;
        };
        let value = if rest.starts_with('[') {
            let Some((_, value)) = rest.split_once(':') else {
                continue;
            };
            value.trim()
        } else {
            let Some(value) = rest.split_whitespace().next() else {
                continue;
            };
            value
        };
        let value = value.split('%').next().unwrap().trim();
        let Ok(address) = value.parse::<IpAddr>() else {
            continue;
        };
        observed = true;
        let usable = match address {
            IpAddr::V4(address) => {
                !address.is_unspecified()
                    && !address.is_loopback()
                    && !address.is_link_local()
                    && !address.is_multicast()
                    && !address.is_broadcast()
            }
            IpAddr::V6(address) => {
                !address.is_unspecified()
                    && !address.is_loopback()
                    && !address.is_unicast_link_local()
                    && !address.is_multicast()
            }
        };
        if !usable || selected[..count].contains(&Some(address)) {
            continue;
        }
        let index = selected[..count]
            .iter()
            .position(|old| old.is_some_and(|old| old > address))
            .unwrap_or(count);
        if index == selected.len() {
            continue;
        }
        let next = (count + 1).min(selected.len());
        selected.copy_within(index..next - 1, index + 1);
        selected[index] = Some(address);
        count = next;
    }
    (
        observed,
        selected[..count]
            .iter()
            .map(|address| address.unwrap())
            .collect(),
    )
}
#[cfg(test)]
#[path = "query/tests.rs"]
mod tests;
/// None is the explicit independent-library path, not a Kernel fallback after
/// failure. A present actual Source context always returns Some, including Err.
pub(crate) fn route_query(arguments: &[String]) -> Option<io::Result<CommandOutput>> {
    current().map(|owner| {
        owner
            .query(tool::Query::Route(arguments.to_vec()))
            .map_err(QueryFailure::into_io)
    })
}
pub(crate) fn route_table(ipv6: bool) -> Option<io::Result<CommandOutput>> {
    current().map(|owner| {
        owner
            .query(tool::Query::RouteTable(ipv6))
            .map_err(QueryFailure::into_io)
    })
}
