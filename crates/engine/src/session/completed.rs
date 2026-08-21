//! Bounded history of completed sessions.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use zero_core::{
    Address, FakeIpReverseStatus, Network, ProtocolType, SessionAuth, TargetHostSource,
};

use crate::observability::SessionOutcome;

use super::{FlowFailureObservation, FlowPathObservation, FlowRouteObservation};

const DEFAULT_COMPLETED_SESSION_CAPACITY: usize = 256;

#[derive(Debug)]
pub struct CompletedSessionHistory {
    capacity: usize,
    inner: Mutex<VecDeque<CompletedSessionRecord>>,
}

impl Default for CompletedSessionHistory {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_COMPLETED_SESSION_CAPACITY,
            inner: Mutex::new(VecDeque::with_capacity(DEFAULT_COMPLETED_SESSION_CAPACITY)),
        }
    }
}

impl CompletedSessionHistory {
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn push(&self, record: CompletedSessionRecord) {
        let mut history = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        history.push_back(record);

        while history.len() > self.capacity {
            history.pop_front();
        }
    }

    pub fn snapshot(&self) -> Vec<CompletedSessionRecord> {
        self.inner
            .lock()
            .expect("completed session lock poisoned")
            .iter()
            .rev()
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedSessionRecord {
    pub id: u64,
    pub revision: u64,
    pub inbound_tag: Option<String>,
    pub outbound_tag: Option<String>,
    pub route: Option<FlowRouteObservation>,
    pub path: FlowPathObservation,
    pub target: Address,
    pub original_target: Option<Address>,
    pub target_host_source: Option<TargetHostSource>,
    pub fake_ip_reverse_status: Option<FakeIpReverseStatus>,
    pub port: u16,
    pub protocol: ProtocolType,
    pub auth: Option<SessionAuth>,
    pub network: Network,
    pub mode: String,
    pub started_at_unix_ms: u64,
    pub last_activity_at_unix_ms: u64,
    pub finished_at_unix_ms: u64,
    pub duration_ms: u64,
    pub bytes_up: u64,
    pub bytes_down: u64,
    pub inbound_rx_bytes: u64,
    pub inbound_tx_bytes: u64,
    pub outbound_rx_bytes: u64,
    pub outbound_tx_bytes: u64,
    pub throughput_up_bps: u64,
    pub throughput_down_bps: u64,
    pub process_id: Option<u32>,
    pub process_name: Option<String>,
    pub process_path: Option<String>,
    pub sni: Option<String>,
    pub source_ip: Option<Address>,
    pub source_port: Option<u16>,
    pub outcome: SessionOutcome,
    /// Why the session ended. `None` = normal peer close / unspecified.
    /// Recognised values: `"manual"` (flows.close), `"idle_timeout"`,
    /// `"upstream_error"`.
    pub close_reason: Option<String>,
    pub failure: Option<FlowFailureObservation>,
}
