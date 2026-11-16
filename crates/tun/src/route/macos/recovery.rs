//! Unknown legacy identities and unconfirmed intents never grant delete authority.
use super::{audit::table, RouteJournal};
use std::{io, path::Path};
pub(super) fn legacy_capture(journal: &RouteJournal) -> bool {
    journal.tun_index == 0 || (!journal.ipv6 && journal.capture_gateway.is_none())
}
pub(super) fn confirm_absent_with(
    path: &Path,
    interface: &str,
    ipv6: bool,
    prefix: &str,
    reason: &str,
    read: impl FnOnce() -> io::Result<Vec<table::RouteEntry>>,
) -> io::Result<()> {
    let prefix: ipnet::IpNet = prefix.parse().map_err(io::Error::other)?;
    let rows = read()?
        .into_iter()
        .filter(|row| row.prefix == prefix && !row.flags.contains('L'))
        .collect::<Vec<_>>();
    if rows.is_empty() {
        return Ok(());
    }
    let family = if ipv6 { "inet6" } else { "inet" };
    Err(io::Error::new(io::ErrorKind::PermissionDenied,format!(
        "TUN_ROUTE_RECOVERY_REQUIRED: {reason}; journal={}; prefix={prefix}; recorded_interface={interface}; observed={rows:?}. Preserve this journal. Inspect with /usr/sbin/netstat -rnW -f {family}; stop the previous tunnel owner or request administrator review. Do not infer the old interface index/gateway or delete matching routes from these fields. After independently verified cleanup, retry TUN start/recovery; the kernel re-reads absence before retiring the record. See release/tun-route-recovery.md",path.display())))
}
pub(super) fn pending(journal: &mut RouteJournal) -> io::Result<()> {
    let path = journal.path.clone();
    let interface = journal.tun_name.clone();
    let ipv6 = journal.ipv6;
    journal.cleanup_pending_with(|prefix| {
        confirm_absent_with(
            &path,
            &interface,
            ipv6,
            prefix,
            "route acquisition was not confirmed",
            || table::read(ipv6),
        )
    })
}
#[cfg(test)]
mod tests;
