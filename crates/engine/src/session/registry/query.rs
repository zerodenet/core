//! Hold actual membership/field guards and freeze scalars once after prepayment.
use super::*;
use crate::runtime_query::{
    row::{Numbers, Row},
    workspace,
};
use std::{cmp::Ordering as Compare, collections::BinaryHeap, sync::MutexGuard};
use zero_runtime::configuration::ControlBudgetPermit;
pub(crate) struct SessionQueryGuard<'a>(MutexGuard<'a, SessionRegistryState>);
pub(crate) struct ActiveQueryRows<'a>(Vec<Frozen<'a>>);
struct Frozen<'a> {
    original: &'a ActiveSessionEntry,
    outbound: MutexGuard<'a, Option<String>>,
    route: MutexGuard<'a, Option<FlowRouteObservation>>,
    path: MutexGuard<'a, FlowPathObservation>,
    numbers: Numbers,
}
impl SessionRegistry {
    pub(crate) fn query_guard(&self) -> SessionQueryGuard<'_> {
        SessionQueryGuard(self.inner.lock().expect("session registry lock poisoned"))
    }
}
struct Selected<'a>(&'a ActiveSessionEntry);
impl PartialEq for Selected<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0.id == other.0.id
    }
}
impl Eq for Selected<'_> {}
impl PartialOrd for Selected<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Compare> {
        Some(self.cmp(other))
    }
}
impl Ord for Selected<'_> {
    fn cmp(&self, other: &Self) -> Compare {
        self.0.id.cmp(&other.0.id)
    }
}
impl SessionQueryGuard<'_> {
    pub(crate) fn contains(&self, id: u64) -> bool {
        self.0.sessions.contains_key(&id)
    }
    pub(crate) fn freeze<'q>(
        &'q self,
        filter: Option<&zero_api::FlowFilter>,
        limit: usize,
        id: Option<u64>,
        workspace: &mut ControlBudgetPermit,
    ) -> std::io::Result<ActiveQueryRows<'q>> {
        let matches = |entry: &ActiveSessionEntry| {
            id.is_none_or(|id| entry.id == id) && matches_filter(entry, filter)
        };
        let count = self
            .0
            .sessions
            .values()
            .filter(|entry| matches(entry))
            .count()
            .min(limit);
        // Both original allocations coexist during freezing. A small limit
        // selects only its lowest actual IDs, without allocating all rows.
        workspace.grow(workspace::bytes::<Selected<'_>>(count)?)?;
        let mut selected = BinaryHeap::new();
        selected
            .try_reserve_exact(count)
            .map_err(std::io::Error::other)?;
        for entry in self.0.sessions.values().filter(|entry| matches(entry)) {
            if selected.len() < count {
                selected.push(Selected(entry));
            } else if let Some(mut greatest) = selected.peek_mut() {
                if entry.id < greatest.0.id {
                    *greatest = Selected(entry);
                }
            }
        }
        let selected = selected.into_sorted_vec();
        let mut rows = workspace::vector::<Frozen<'_>>(count, workspace)?;
        for selected in selected {
            let entry = selected.0;
            let now = unix_timestamp_ms();
            let rx = entry.inbound_rx_bytes.load(Ordering::Relaxed);
            let tx = entry.inbound_tx_bytes.load(Ordering::Relaxed);
            let remote_rx = entry.outbound_rx_bytes.load(Ordering::Relaxed);
            let remote_tx = entry.outbound_tx_bytes.load(Ordering::Relaxed);
            let throughput =
                entry
                    .throughput_sampler
                    .snapshot(now, rx.max(remote_tx), remote_rx.max(tx));
            let numbers = Numbers {
                revision: entry.revision.load(Ordering::Relaxed),
                last_activity: entry.last_activity_at_unix_ms.load(Ordering::Relaxed),
                rx,
                tx,
                remote_rx,
                remote_tx,
                up_bps: throughput.up_bps,
                down_bps: throughput.down_bps,
                sampled_at: now,
            };
            rows.push(Frozen {
                original: entry,
                outbound: entry
                    .outbound_tag
                    .lock()
                    .expect("outbound tag lock poisoned"),
                route: entry.route.lock().expect("session route lock poisoned"),
                path: entry.path.lock().expect("session path lock poisoned"),
                numbers,
            });
        }
        Ok(ActiveQueryRows(rows))
    }
}
fn matches_filter(entry: &ActiveSessionEntry, filter: Option<&zero_api::FlowFilter>) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    filter
        .inbound_tag
        .as_deref()
        .is_none_or(|expected| entry.inbound_tag.as_deref() == Some(expected))
        && filter.principal_key.as_deref().is_none_or(|expected| {
            entry
                .auth
                .as_ref()
                .and_then(|auth| auth.principal_key.as_deref())
                == Some(expected)
        })
        && filter.network.is_none_or(|expected| {
            matches!(
                (expected, entry.network),
                (zero_api::Network::Tcp, Network::Tcp) | (zero_api::Network::Udp, Network::Udp)
            )
        })
}
impl ActiveQueryRows<'_> {
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }
    pub(crate) fn iter(&self) -> impl Iterator<Item = Row<'_>> {
        self.0.iter().map(|row| {
            let entry = row.original;
            Row {
                id: entry.id,
                inbound: entry.inbound_tag.as_deref(),
                outbound: row.outbound.as_deref(),
                route: row.route.as_ref(),
                path: &row.path,
                target: &entry.target,
                original_target: entry.original_target.as_ref(),
                host_source: entry.target_host_source,
                reverse_status: entry.fake_ip_reverse_status,
                port: entry.port,
                protocol: entry.protocol,
                auth: entry.auth.as_ref(),
                network: entry.network,
                mode: &entry.mode,
                started_at: entry.started_at_unix_ms,
                numbers: row.numbers,
                process_id: entry.process_id,
                process_name: entry.process_name.as_deref(),
                process_path: entry.process_path.as_deref(),
                sni: entry.sni.as_deref(),
                source_ip: entry.source_ip.as_ref(),
                source_port: entry.source_port,
                end: None,
            }
        })
    }
}
