//! Completed records are immutable under their actual history guard.
use super::*;
use crate::runtime_query::row::{End, Numbers, Row};
use std::sync::MutexGuard;
pub(crate) struct CompletedQueryGuard<'a>(MutexGuard<'a, VecDeque<CompletedSessionRecord>>);
impl CompletedSessionHistory {
    pub(crate) fn query_guard(&self) -> CompletedQueryGuard<'_> {
        CompletedQueryGuard(self.inner.lock().expect("completed session lock poisoned"))
    }
}
impl CompletedQueryGuard<'_> {
    pub(crate) fn iter<'a>(
        &'a self,
        filter: Option<&'a zero_api::FlowFilter>,
        limit: usize,
        id: Option<u64>,
    ) -> impl Iterator<Item = Row<'a>> {
        self.0
            .iter()
            .rev()
            .filter(move |record| {
                id.is_none_or(|id| record.id == id)
                    && filter.is_none_or(|filter| {
                        filter
                            .inbound_tag
                            .as_deref()
                            .is_none_or(|expected| record.inbound_tag.as_deref() == Some(expected))
                            && filter.principal_key.as_deref().is_none_or(|expected| {
                                record
                                    .auth
                                    .as_ref()
                                    .and_then(|auth| auth.principal_key.as_deref())
                                    == Some(expected)
                            })
                            && filter.network.is_none_or(|expected| {
                                matches!(
                                    (expected, record.network),
                                    (zero_api::Network::Tcp, Network::Tcp)
                                        | (zero_api::Network::Udp, Network::Udp)
                                )
                            })
                    })
            })
            .take(limit)
            .map(|record| Row {
                id: record.id,
                inbound: record.inbound_tag.as_deref(),
                outbound: record.outbound_tag.as_deref(),
                route: record.route.as_ref(),
                path: &record.path,
                target: &record.target,
                original_target: record.original_target.as_ref(),
                host_source: record.target_host_source,
                reverse_status: record.fake_ip_reverse_status,
                port: record.port,
                protocol: record.protocol,
                auth: record.auth.as_ref(),
                network: record.network,
                mode: &record.mode,
                started_at: record.started_at_unix_ms,
                numbers: Numbers {
                    revision: record.revision,
                    last_activity: record.last_activity_at_unix_ms,
                    rx: record.inbound_rx_bytes,
                    tx: record.inbound_tx_bytes,
                    remote_rx: record.outbound_rx_bytes,
                    remote_tx: record.outbound_tx_bytes,
                    up_bps: record.throughput_up_bps,
                    down_bps: record.throughput_down_bps,
                    sampled_at: record.finished_at_unix_ms,
                },
                process_id: record.process_id,
                process_name: record.process_name.as_deref(),
                process_path: record.process_path.as_deref(),
                sni: record.sni.as_deref(),
                source_ip: record.source_ip.as_ref(),
                source_port: record.source_port,
                end: Some(End {
                    finished_at: record.finished_at_unix_ms,
                    duration: record.duration_ms,
                    outcome: record.outcome,
                    close_reason: record.close_reason.as_deref(),
                    failure: record.failure.as_ref(),
                }),
            })
    }
}
