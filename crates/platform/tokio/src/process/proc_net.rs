//! Pure `/proc/net/{tcp,udp,tcp6,udp6}` row matcher shared with cross-platform tests.

/// Inspects a single non-header `/proc/net/*` row for `(ip | wildcard):port`.
///
/// Returns:
/// - `Some((true, inode))` when `local_ip == ip` and `inode != 0` (exact match).
/// - `Some((false, inode))` when `local_ip == wildcard` and `inode != 0` (fallback).
/// - `None` for non-matching rows or kernel `TIME_WAIT`/orphan rows (`inode == 0`).
pub fn match_proc_net_row(line: &str, ip: &str, wildcard: &str, port: &str) -> Option<(bool, u64)> {
    let mut fields = line.split_whitespace();
    let local = fields.nth(1)?;
    let (local_ip, local_port) = local.split_once(':')?;
    if (local_ip != ip && local_ip != wildcard) || local_port != port {
        return None;
    }
    let inode = fields
        .nth(7)
        .and_then(|inode| inode.parse::<u64>().ok())
        .filter(|inode| *inode != 0)?;
    Some((local_ip == ip, inode))
}
