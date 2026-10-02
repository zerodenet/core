//! /proc/net/dev folds rx_missed_errors into its receive drop column.
use super::{invalid, InterfaceStatistics, MAX_BYTES, MAX_INTERFACES};
use std::io;

pub(super) fn parse(
    text: &str,
    mut index: impl FnMut(&str) -> io::Result<u32>,
) -> io::Result<Vec<InterfaceStatistics>> {
    if text.len() > MAX_BYTES {
        return Err(invalid("interface statistics exceed byte limit"));
    }
    let mut result = Vec::new();
    let mut names = std::collections::BTreeSet::new();
    let mut lines = text.lines();
    if !lines
        .next()
        .is_some_and(|s| s.contains("Inter-") && s.contains("Receive"))
        || !lines
            .next()
            .is_some_and(|s| s.contains("bytes") && s.contains("drop"))
    {
        return Err(invalid("missing interface statistics headers"));
    }
    for line in lines.filter(|s| !s.trim().is_empty()) {
        let (name, fields) = line
            .rsplit_once(':')
            .ok_or_else(|| invalid("invalid interface statistics row"))?;
        let name = name.trim();
        if name.is_empty()
            || name.len() > 128
            || !names.insert(name)
            || result.len() >= MAX_INTERFACES
        {
            return Err(invalid("invalid or excessive interface identities"));
        }
        let values: Vec<u64> = fields
            .split_whitespace()
            .map(|v| v.parse().map_err(|_| invalid("invalid interface counter")))
            .collect::<io::Result<_>>()?;
        if values.len() != 16 {
            return Err(invalid("unexpected interface counter columns"));
        }
        let index = index(name)?;
        if index == 0 {
            return Err(invalid("interface disappeared during capture"));
        }
        result.push(InterfaceStatistics {
            name: name.into(),
            index,
            accounting_basis: "linux_proc_net_dev_rx_drop_includes_missed_host_interface_only",
            rx_bytes: values[0],
            tx_bytes: values[8],
            rx_packets: values[1],
            tx_packets: values[9],
            rx_dropped_packets: Some(values[3]),
            tx_dropped_packets: Some(values[11]),
            rx_errors: Some(values[2]),
            tx_errors: Some(values[10]),
        });
    }
    Ok(result)
}
#[cfg(test)]
#[path = "../../tests/network_statistics/procfs.rs"]
mod tests;
