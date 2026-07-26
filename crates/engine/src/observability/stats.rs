//! Engine-wide session, traffic, and upstream statistics.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use zero_api::{OutboundTrafficStats, StatsSnapshot, UdpUpstreamStats};

#[derive(Debug, Default)]
pub struct EngineStats {
    total_started: AtomicU64,
    active_sessions: AtomicU64,
    completed_sessions: AtomicU64,
    failed_sessions: AtomicU64,
    blocked_sessions: AtomicU64,
    direct_sessions: AtomicU64,
    chained_sessions: AtomicU64,
    udp_upstream_active_associations: AtomicU64,
    udp_upstream_created_associations: AtomicU64,
    udp_upstream_reused_associations: AtomicU64,
    udp_upstream_closed_associations: AtomicU64,
    udp_upstream_idle_timeouts: AtomicU64,
    udp_upstream_dropped_associations: AtomicU64,
    udp_upstream_failed_association_attempts: AtomicU64,
    udp_upstream_send_failures: AtomicU64,
    udp_upstream_recv_failures: AtomicU64,
    udp_upstream_packets_sent: AtomicU64,
    udp_upstream_packets_received: AtomicU64,
    bytes_up: AtomicU64,
    bytes_down: AtomicU64,
    per_outbound: Mutex<HashMap<String, PerOutboundStats>>,
}

impl EngineStats {
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Record completed flow traffic — global + per-outbound.
    pub fn record_traffic(&self, outbound_tag: Option<&str>, bytes_up: u64, bytes_down: u64) {
        self.bytes_up.fetch_add(bytes_up, Ordering::Relaxed);
        self.bytes_down.fetch_add(bytes_down, Ordering::Relaxed);
        if let Some(tag) = outbound_tag {
            let mut map = self
                .per_outbound
                .lock()
                .expect("per-outbound lock poisoned");
            let entry = map.entry(tag.to_owned()).or_default();
            entry.flows.fetch_add(1, Ordering::Relaxed);
            entry.bytes_up.fetch_add(bytes_up, Ordering::Relaxed);
            entry.bytes_down.fetch_add(bytes_down, Ordering::Relaxed);
        }
    }

    pub fn snapshot(&self) -> StatsSnapshot {
        let per_outbound = self
            .per_outbound
            .lock()
            .expect("per-outbound lock poisoned")
            .iter()
            .map(|(tag, s)| {
                (
                    tag.clone(),
                    OutboundTrafficStats {
                        flows: s.flows.load(Ordering::Relaxed),
                        bytes_up: s.bytes_up.load(Ordering::Relaxed),
                        bytes_down: s.bytes_down.load(Ordering::Relaxed),
                    },
                )
            })
            .collect();

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
            per_outbound,
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

    pub fn record_start(&self) {
        self.total_started.fetch_add(1, Ordering::Relaxed);
        self.active_sessions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_finish(&self, outcome: SessionOutcome) {
        self.active_sessions.fetch_sub(1, Ordering::Relaxed);

        match outcome {
            SessionOutcome::DirectRelayed => {
                self.completed_sessions.fetch_add(1, Ordering::Relaxed);
                self.direct_sessions.fetch_add(1, Ordering::Relaxed);
            }
            SessionOutcome::ChainedRelayed => {
                self.completed_sessions.fetch_add(1, Ordering::Relaxed);
                self.chained_sessions.fetch_add(1, Ordering::Relaxed);
            }
            SessionOutcome::Blocked => {
                self.blocked_sessions.fetch_add(1, Ordering::Relaxed);
            }
            SessionOutcome::Failed => {
                self.failed_sessions.fetch_add(1, Ordering::Relaxed);
            }
            SessionOutcome::Cancelled => {
                self.completed_sessions.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn record_udp_upstream_association_created(&self) {
        self.udp_upstream_active_associations
            .fetch_add(1, Ordering::Relaxed);
        self.udp_upstream_created_associations
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_udp_upstream_association_reused(&self) {
        self.udp_upstream_reused_associations
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_udp_upstream_association_closed(&self) {
        self.udp_upstream_active_associations
            .fetch_sub(1, Ordering::Relaxed);
        self.udp_upstream_closed_associations
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_udp_upstream_association_idle_timeout(&self) {
        self.udp_upstream_active_associations
            .fetch_sub(1, Ordering::Relaxed);
        self.udp_upstream_idle_timeouts
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_udp_upstream_association_dropped(&self) {
        self.udp_upstream_active_associations
            .fetch_sub(1, Ordering::Relaxed);
        self.udp_upstream_dropped_associations
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_udp_upstream_association_failed(&self) {
        self.udp_upstream_failed_association_attempts
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_udp_upstream_send_failure(&self) {
        self.udp_upstream_send_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_udp_upstream_recv_failure(&self) {
        self.udp_upstream_recv_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_udp_upstream_packet_sent(&self) {
        self.udp_upstream_packets_sent
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_udp_upstream_packet_received(&self) {
        self.udp_upstream_packets_received
            .fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionOutcome {
    DirectRelayed,
    ChainedRelayed,
    Blocked,
    Failed,
    Cancelled,
}

impl SessionOutcome {
    pub fn kind(self) -> &'static str {
        match self {
            SessionOutcome::DirectRelayed => "direct_relayed",
            SessionOutcome::ChainedRelayed => "chained_relayed",
            SessionOutcome::Blocked => "blocked",
            SessionOutcome::Failed => "failed",
            SessionOutcome::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Default)]
struct PerOutboundStats {
    flows: AtomicU64,
    bytes_up: AtomicU64,
    bytes_down: AtomicU64,
}
