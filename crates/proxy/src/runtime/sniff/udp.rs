use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Instant;

use zero_core::{Address, InboundUdpDispatch, TargetHostSource};
use zero_transport::quic_initial::{
    looks_like_client_initial, QuicInitialOutcome, QuicInitialSniffer,
};

use super::{
    normalize_sniffed_domain, sniff_tls_handshake, SniffProgress, SniffedDomain, SniffedProtocol,
    SniffingPolicy, SNIFF_TIMEOUT,
};

const MAX_PENDING_FLOWS: usize = 32;
const MAX_DECISIONS: usize = 64;
const MAX_BUFFERED_DATAGRAMS: usize = 8;
const MAX_BUFFERED_BYTES: usize = 64 * 1024;
const MAX_QUEUED_DATAGRAMS: usize = 64;
const MAX_QUEUED_BYTES: usize = 256 * 1024;
const MAX_DECODED_DATAGRAM_BYTES: usize = 8 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct FlowKey {
    target: Address,
    port: u16,
    client_session_id: Option<u64>,
}

impl FlowKey {
    fn from_dispatch(dispatch: &InboundUdpDispatch) -> Self {
        Self {
            target: dispatch.target().clone(),
            port: dispatch.port(),
            client_session_id: dispatch.client_session_id(),
        }
    }
}

struct PendingQuic {
    sniffer: QuicInitialSniffer,
    datagrams: VecDeque<BufferedDispatch>,
    buffered_bytes: usize,
    deadline: Instant,
    fake_dns_fallback: bool,
}

struct BufferedDispatch {
    sequence: u64,
    dispatch: InboundUdpDispatch,
}

#[derive(Clone)]
enum Decision {
    Domain {
        sniffed: SniffedDomain,
        fake_dns_fallback: bool,
    },
    Fallback,
}

pub(crate) struct UdpSniffingState {
    policy: SniffingPolicy,
    runtime: crate::runtime::udp_ingress::UdpIngressRuntime,
    pending: HashMap<FlowKey, PendingQuic>,
    decisions: HashMap<FlowKey, Decision>,
    ready: BTreeMap<u64, InboundUdpDispatch>,
    next_sequence: u64,
    next_emit: u64,
    queued_datagrams: usize,
    queued_bytes: usize,
}

impl UdpSniffingState {
    pub(crate) fn new(
        policy: SniffingPolicy,
        runtime: crate::runtime::udp_ingress::UdpIngressRuntime,
    ) -> Self {
        Self {
            policy,
            runtime,
            pending: HashMap::new(),
            decisions: HashMap::new(),
            ready: BTreeMap::new(),
            next_sequence: 0,
            next_emit: 0,
            queued_datagrams: 0,
            queued_bytes: 0,
        }
    }

    pub(crate) fn pop_ready(&mut self) -> Option<InboundUdpDispatch> {
        let dispatch = self.ready.remove(&self.next_emit)?;
        self.next_emit = self.next_emit.saturating_add(1);
        self.queued_datagrams = self.queued_datagrams.saturating_sub(1);
        self.queued_bytes = self.queued_bytes.saturating_sub(dispatch.payload().len());
        Some(dispatch)
    }

    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.pending.values().map(|pending| pending.deadline).min()
    }

    pub(crate) fn release_for_capacity(&mut self) -> bool {
        if self.queued_datagrams < MAX_QUEUED_DATAGRAMS
            && self.queued_bytes <= MAX_QUEUED_BYTES - MAX_DECODED_DATAGRAM_BYTES
        {
            return false;
        }
        let oldest = self
            .pending
            .iter()
            .filter_map(|(key, pending)| {
                pending
                    .datagrams
                    .front()
                    .map(|buffered| (buffered.sequence, key.clone()))
            })
            .min_by_key(|(sequence, _)| *sequence)
            .map(|(_, key)| key);
        let Some(oldest) = oldest else {
            return false;
        };
        self.resolve(oldest, Decision::Fallback);
        true
    }

    pub(crate) async fn observe(&mut self, dispatch: InboundUdpDispatch) {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.queued_datagrams = self.queued_datagrams.saturating_add(1);
        self.queued_bytes = self.queued_bytes.saturating_add(dispatch.payload().len());
        let dispatch = if self.policy.enabled() {
            dispatch.with_skip_fake_ip_restore()
        } else {
            dispatch
        };
        let metadata = self
            .policy
            .fake_dns_metadata(self.runtime.resolver(), dispatch.target())
            .await;
        let fake_dns_fallback = match metadata {
            super::FakeDnsMetadata::Domain(sniffed) => {
                let decision = if self.policy.accepts(&sniffed) {
                    Decision::Domain {
                        sniffed,
                        fake_dns_fallback: false,
                    }
                } else {
                    Decision::Fallback
                };
                let key = FlowKey::from_dispatch(&dispatch);
                if self.decisions.len() < MAX_DECISIONS {
                    self.decisions.insert(key, decision.clone());
                }
                let dispatch = self.apply_decision(dispatch, decision);
                self.ready.insert(sequence, dispatch);
                return;
            }
            super::FakeDnsMetadata::ContentFallback => true,
            super::FakeDnsMetadata::None => false,
        };
        if !self.policy.sniffs_quic(fake_dns_fallback) {
            self.ready.insert(sequence, dispatch);
            return;
        }
        let key = FlowKey::from_dispatch(&dispatch);
        if let Some(decision) = self.decisions.get(&key).cloned() {
            let dispatch = self.apply_decision(dispatch, decision);
            self.ready.insert(sequence, dispatch);
            return;
        }
        if !self.pending.contains_key(&key) && !looks_like_client_initial(dispatch.payload()) {
            if self.decisions.len() < MAX_DECISIONS {
                self.decisions.insert(key, Decision::Fallback);
            }
            self.ready.insert(sequence, dispatch);
            return;
        }
        if !self.pending.contains_key(&key) && self.pending.len() >= MAX_PENDING_FLOWS {
            self.ready.insert(sequence, dispatch);
            return;
        }

        let pending = self
            .pending
            .entry(key.clone())
            .or_insert_with(|| PendingQuic {
                sniffer: QuicInitialSniffer::new(),
                datagrams: VecDeque::new(),
                buffered_bytes: 0,
                deadline: Instant::now() + SNIFF_TIMEOUT,
                fake_dns_fallback,
            });
        pending.buffered_bytes = pending
            .buffered_bytes
            .saturating_add(dispatch.payload().len());
        let outcome = pending.sniffer.ingest(dispatch.payload());
        pending
            .datagrams
            .push_back(BufferedDispatch { sequence, dispatch });
        let fake_dns_fallback = pending.fake_dns_fallback;
        if pending.datagrams.len() > MAX_BUFFERED_DATAGRAMS
            || pending.buffered_bytes > MAX_BUFFERED_BYTES
        {
            self.resolve(key, Decision::Fallback);
            return;
        }
        match outcome {
            QuicInitialOutcome::Pending => {}
            QuicInitialOutcome::ClientHello(client_hello) => {
                let decision = match sniff_tls_handshake(&client_hello) {
                    SniffProgress::Domain(sniffed) => normalize_sniffed_domain(sniffed.domain)
                        .map(|domain| SniffedDomain {
                            domain,
                            protocol: SniffedProtocol::Quic,
                            source: TargetHostSource::QuicSni,
                        })
                        .filter(|sniffed| {
                            self.policy
                                .accepts_with_fake_dns_fallback(sniffed, fake_dns_fallback)
                        })
                        .map_or(Decision::Fallback, |sniffed| Decision::Domain {
                            sniffed,
                            fake_dns_fallback,
                        }),
                    SniffProgress::Pending | SniffProgress::NoMatch => Decision::Fallback,
                };
                self.resolve(key, decision);
            }
            QuicInitialOutcome::NotInitial | QuicInitialOutcome::Rejected => {
                self.resolve(key, Decision::Fallback);
            }
        }
    }

    pub(crate) fn flush_expired(&mut self, now: Instant) {
        let expired = self
            .pending
            .iter()
            .filter_map(|(key, pending)| (pending.deadline <= now).then_some(key.clone()))
            .collect::<Vec<_>>();
        for key in expired {
            self.resolve(key, Decision::Fallback);
        }
    }

    pub(crate) fn flush_all(&mut self) {
        let pending = self.pending.keys().cloned().collect::<Vec<_>>();
        for key in pending {
            self.resolve(key, Decision::Fallback);
        }
    }

    fn resolve(&mut self, key: FlowKey, decision: Decision) {
        let Some(mut pending) = self.pending.remove(&key) else {
            return;
        };
        if self.decisions.len() < MAX_DECISIONS {
            self.decisions.insert(key, decision.clone());
        }
        while let Some(buffered) = pending.datagrams.pop_front() {
            let dispatch = self.apply_decision(buffered.dispatch, decision.clone());
            self.ready.insert(buffered.sequence, dispatch);
        }
    }

    fn apply_decision(
        &self,
        dispatch: InboundUdpDispatch,
        decision: Decision,
    ) -> InboundUdpDispatch {
        match decision {
            Decision::Domain {
                sniffed,
                fake_dns_fallback,
            } => dispatch.with_sniffed_domain(
                sniffed.domain,
                sniffed.source,
                self.policy.route_only()
                    && sniffed.protocol != SniffedProtocol::FakeDns
                    && !fake_dns_fallback,
            ),
            Decision::Fallback => dispatch,
        }
    }
}

#[cfg(test)]
#[path = "udp/tests.rs"]
mod tests;
