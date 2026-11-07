//! Freeze numeric statistics once; the actual tag map stays borrowed.
use super::*;
use crate::runtime_query::workspace;
use serde::{ser::SerializeStruct, Serialize};
use std::sync::MutexGuard;
use zero_runtime::configuration::ControlBudgetPermit;
pub(crate) struct StatsQueryGuard<'a> {
    original: &'a EngineStats,
    map: Option<MutexGuard<'a, HashMap<String, PerOutboundStats>>>,
}
pub(crate) struct StatsQueryView<'a> {
    pub(crate) header: StatsSnapshot,
    rows: Vec<(&'a str, OutboundTrafficStats)>,
}
impl EngineStats {
    pub(crate) fn query_guard(&self, per_outbound: bool) -> StatsQueryGuard<'_> {
        StatsQueryGuard {
            original: self,
            map: per_outbound.then(|| {
                self.per_outbound
                    .lock()
                    .expect("per-outbound lock poisoned")
            }),
        }
    }
    fn query_header(&self) -> StatsSnapshot {
        StatsSnapshot {
            total_started: self.total_started.load(Ordering::Relaxed),
            active_sessions: self.active_sessions.load(Ordering::Relaxed),
            completed_sessions: self.completed_sessions.load(Ordering::Relaxed),
            failed_sessions: self.failed_sessions.load(Ordering::Relaxed),
            blocked_sessions: self.blocked_sessions.load(Ordering::Relaxed),
            direct_sessions: self.direct_sessions.load(Ordering::Relaxed),
            chained_sessions: self.chained_sessions.load(Ordering::Relaxed),
            bytes_up: self.bytes_up.load(Ordering::Relaxed),
            bytes_down: self.bytes_down.load(Ordering::Relaxed),
            per_outbound: Vec::new(),
            udp_upstream: UdpUpstreamStats {
                active_associations: self
                    .udp_upstream_active_associations
                    .load(Ordering::Relaxed),
                created_associations: self
                    .udp_upstream_created_associations
                    .load(Ordering::Relaxed),
                reused_associations: self
                    .udp_upstream_reused_associations
                    .load(Ordering::Relaxed),
                closed_associations: self
                    .udp_upstream_closed_associations
                    .load(Ordering::Relaxed),
                idle_timeouts: self.udp_upstream_idle_timeouts.load(Ordering::Relaxed),
                dropped_associations: self
                    .udp_upstream_dropped_associations
                    .load(Ordering::Relaxed),
                failed_association_attempts: self
                    .udp_upstream_failed_association_attempts
                    .load(Ordering::Relaxed),
                send_failures: self.udp_upstream_send_failures.load(Ordering::Relaxed),
                recv_failures: self.udp_upstream_recv_failures.load(Ordering::Relaxed),
                packets_sent: self.udp_upstream_packets_sent.load(Ordering::Relaxed),
                packets_received: self.udp_upstream_packets_received.load(Ordering::Relaxed),
            },
        }
    }
}
impl StatsQueryGuard<'_> {
    pub(crate) fn freeze(
        &self,
        workspace: &mut ControlBudgetPermit,
    ) -> std::io::Result<StatsQueryView<'_>> {
        let count = self.map.as_ref().map_or(0, |map| map.len());
        let mut rows = workspace::vector::<(&str, OutboundTrafficStats)>(count, workspace)?;
        let header = self.original.query_header();
        if let Some(map) = &self.map {
            for (tag, entry) in map.iter() {
                rows.push((
                    tag.as_str(),
                    OutboundTrafficStats {
                        flows: entry.flows.load(Ordering::Relaxed),
                        bytes_up: entry.bytes_up.load(Ordering::Relaxed),
                        bytes_down: entry.bytes_down.load(Ordering::Relaxed),
                    },
                ));
            }
        }
        Ok(StatsQueryView { header, rows })
    }
}
impl Serialize for StatsQueryView<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let header = &self.header;
        let mut out = serializer.serialize_struct("StatsSnapshot", 11)?;
        out.serialize_field("total_started", &header.total_started)?;
        out.serialize_field("active_sessions", &header.active_sessions)?;
        out.serialize_field("completed_sessions", &header.completed_sessions)?;
        out.serialize_field("failed_sessions", &header.failed_sessions)?;
        out.serialize_field("blocked_sessions", &header.blocked_sessions)?;
        out.serialize_field("direct_sessions", &header.direct_sessions)?;
        out.serialize_field("chained_sessions", &header.chained_sessions)?;
        out.serialize_field("bytes_up", &header.bytes_up)?;
        out.serialize_field("bytes_down", &header.bytes_down)?;
        out.serialize_field("per_outbound", &self.rows)?;
        out.serialize_field("udp_upstream", &header.udp_upstream)?;
        out.end()
    }
}
