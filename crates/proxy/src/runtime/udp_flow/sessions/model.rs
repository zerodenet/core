use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;

use zero_core::{Address, Session};
use zero_engine::PassiveRelaySelection;
use zero_engine::{CompletedSessionRecord, FlowFailureObservation, SessionHandle, SessionOutcome};

use crate::runtime::udp_flow::outbound::UdpFlowOutbound;
use crate::runtime::udp_flow::rate_limit::UdpFlowRateLimiters;
use crate::runtime::udp_flow::snapshot::UdpFlowSnapshot;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct UdpFlowKey {
    pub(super) target: Address,
    pub(super) port: u16,
    /// Per-client-session isolation key.
    ///
    /// When `Some`, flows with the same `(target, port)` but different
    /// `client_session_id` are treated as independent relay sessions (SIP022
    /// 3.2.4). When `None` (legacy AEAD, non-SS protocols), the existing
    /// `(target, port)` keying is preserved.
    client_session_id: Option<u64>,
}

impl UdpFlowKey {
    pub(crate) fn new(target: &Address, port: u16, client_session_id: Option<u64>) -> Self {
        Self {
            target: target.clone(),
            port,
            client_session_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct UdpUpstreamResponseKey {
    outbound_tag: String,
    target: Address,
    port: u16,
}

impl UdpUpstreamResponseKey {
    pub(super) fn new(outbound_tag: &str, target: &Address, port: u16) -> Self {
        Self {
            outbound_tag: outbound_tag.to_owned(),
            target: target.clone(),
            port,
        }
    }
}

#[derive(Debug)]
pub(crate) struct CompletedUdpFlow {
    pub(crate) record: CompletedSessionRecord,
    pub(crate) upstream: Option<(String, u16)>,
    pub(crate) passive_relay_selections: Vec<PassiveRelaySelection>,
    pub(crate) passive_health_confirmed: bool,
}

#[derive(Debug, Default)]
pub(crate) struct UdpSessionFlows {
    pub(super) flows: HashMap<UdpFlowKey, UdpFlow>,
    pub(super) direct_by_sender: HashMap<SocketAddr, UdpFlowKey>,
    pub(super) upstream_by_response: HashMap<UdpUpstreamResponseKey, UdpFlowKey>,
}

#[derive(Debug)]
pub(super) struct UdpFlow {
    pub(super) session: Session,
    pub(super) handle: SessionHandle,
    pub(super) outbound: UdpFlowOutbound,
    pub(super) passive_relay_selections: Vec<PassiveRelaySelection>,
    pub(super) passive_health_confirmed: AtomicBool,
    pub(super) rate_limiters: UdpFlowRateLimiters,
}

impl UdpFlow {
    pub(super) fn snapshot(&self, key: UdpFlowKey) -> UdpFlowSnapshot {
        UdpFlowSnapshot {
            key,
            session: self.session.clone(),
            outbound: self.outbound.clone(),
            passive_relay_selections: self.passive_relay_selections.clone(),
            rate_limiters: self.rate_limiters.clone(),
        }
    }

    pub(super) fn finish(mut self, outcome: SessionOutcome) -> CompletedUdpFlow {
        let upstream = self.outbound.completion().upstream;
        let passive_health_confirmed = self
            .passive_health_confirmed
            .load(std::sync::atomic::Ordering::Acquire);
        let record = self
            .handle
            .finish(outcome)
            .expect("udp flow should be active before finish");

        CompletedUdpFlow {
            record,
            upstream,
            passive_relay_selections: self.passive_relay_selections,
            passive_health_confirmed,
        }
    }

    pub(super) fn finish_success(self) -> CompletedUdpFlow {
        let outcome = self.outbound.completion().success_outcome;
        self.finish(outcome)
    }

    pub(super) fn finish_with_failure(
        mut self,
        failure: FlowFailureObservation,
    ) -> CompletedUdpFlow {
        let upstream = self.outbound.completion().upstream;
        let passive_health_confirmed = self
            .passive_health_confirmed
            .load(std::sync::atomic::Ordering::Acquire);
        let record = self
            .handle
            .finish_with_failure("upstream_error", failure)
            .expect("udp flow should be active before failure");

        CompletedUdpFlow {
            record,
            upstream,
            passive_relay_selections: self.passive_relay_selections,
            passive_health_confirmed,
        }
    }

    pub(super) fn finish_cancelled(mut self) -> CompletedUdpFlow {
        let upstream = self.outbound.completion().upstream;
        let passive_health_confirmed = self
            .passive_health_confirmed
            .load(std::sync::atomic::Ordering::Acquire);
        let reason = self
            .handle
            .cancellation_reason()
            .unwrap_or_else(|| "cancelled".to_owned());
        let record = self
            .handle
            .finish_with_reason(SessionOutcome::Cancelled, Some(reason))
            .expect("cancelled udp flow should be active before finish");

        CompletedUdpFlow {
            record,
            upstream,
            passive_relay_selections: self.passive_relay_selections,
            passive_health_confirmed,
        }
    }
}
