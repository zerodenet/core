use std::time::Instant;
mod domain;
mod key;
mod observe;
pub(crate) mod queue;
pub(crate) mod rows;
mod source;
use super::inline::InlineName;
use super::{SniffedProtocol, SniffingPolicy};
use domain::Held;
use key::FlowKey;
use queue::Packets;
use rows::Rows;
use zero_core::{InboundUdpDispatch, TargetHostSource};
const MAX_PENDING_FLOWS: usize = 32;
const MAX_DECISIONS: usize = 64;
const MAX_BUFFERED_DATAGRAMS: usize = 8;
const MAX_BUFFERED_BYTES: usize = 64 * 1024;
const MAX_QUEUED_DATAGRAMS: usize = 64;
const MAX_QUEUED_BYTES: usize = 256 * 1024;
const MAX_DECODED_DATAGRAM_BYTES: usize = 8 * 1024;
struct PendingQuic {
    id: u64,
    sniffer: zero_transport::quic_initial::QuicInitialSniffer,
    datagrams: usize,
    buffered_bytes: usize,
    deadline: Instant,
    fake_dns_fallback: bool,
}
struct PendingRow {
    key: FlowKey,
    value: PendingQuic,
}
struct DecisionRow {
    key: FlowKey,
    value: Decision,
}
#[derive(Clone, Copy)]
enum Decision {
    Domain {
        name: InlineName,
        protocol: SniffedProtocol,
        source: TargetHostSource,
        fake_dns_fallback: bool,
    },
    Fallback,
}
pub(crate) struct UdpSniffingState {
    policy: SniffingPolicy,
    memory: Option<zero_platform_tokio::contract::component::PacketDriverMetadataView>,
    runtime: crate::runtime::udp_ingress::UdpIngressRuntime,
    pending: Rows<PendingRow>,
    decisions: Rows<DecisionRow>,
    packets: Packets<Held>,
    output_pressure: Option<zero_traits::packet_metadata::PacketMemoryError>,
    next_sequence: u64,
    next_emit: u64,
    queued_datagrams: usize,
    queued_bytes: usize,
}
impl UdpSniffingState {
    pub(crate) fn pop_ready(&mut self) -> Option<InboundUdpDispatch> {
        let held = self.packets.current_mut(self.next_emit)?;
        if let Err(source) = held.prepare(self.memory.as_ref(), &self.policy) {
            self.output_pressure = Some(source);
            tracing::trace!(%source,"same queued known Domain awaits actual Root capacity");
            return None;
        }
        self.output_pressure = None;
        let dispatch = self.packets.pop(self.next_emit)?.into_dispatch();
        self.next_emit = self.next_emit.saturating_add(1);
        self.queued_datagrams = self.queued_datagrams.saturating_sub(1);
        self.queued_bytes = self.queued_bytes.saturating_sub(dispatch.payload().len());
        Some(dispatch)
    }
    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.pending.iter().map(|row| row.value.deadline).min()
    }
    pub(crate) fn release_for_capacity(&mut self) -> bool {
        if self.queued_datagrams < MAX_QUEUED_DATAGRAMS
            && self.queued_bytes <= MAX_QUEUED_BYTES - MAX_DECODED_DATAGRAM_BYTES
        {
            return false;
        }
        let Some(index) = self
            .pending
            .iter()
            .enumerate()
            .min_by_key(|(_, row)| row.value.id)
            .map(|(index, _)| index)
        else {
            return false;
        };
        self.resolve(index, Decision::Fallback);
        true
    }
    pub(crate) fn flush_expired(&mut self, now: Instant) {
        while let Some(index) = self
            .pending
            .iter()
            .position(|row| row.value.deadline <= now)
        {
            self.resolve(index, Decision::Fallback);
        }
    }
    pub(crate) fn flush_all(&mut self) {
        while !self.pending.is_empty() {
            self.resolve(0, Decision::Fallback);
        }
    }
    fn push(&mut self, sequence: u64, pending: Option<u64>, dispatch: InboundUdpDispatch) {
        self.push_decision(sequence, pending, dispatch, Decision::Fallback);
    }
    fn push_decision(
        &mut self,
        sequence: u64,
        pending: Option<u64>,
        dispatch: InboundUdpDispatch,
        decision: Decision,
    ) {
        self.packets
            .push(sequence, pending, Held::new(dispatch, decision));
    }
    pub(crate) fn row_pressure(&self) -> bool {
        self.packets.row_pressure() || self.output_pressure.is_some()
    }
    pub(crate) async fn wait_capacity(&mut self) {
        if self.output_pressure.is_some() {
            let memory = self
                .memory
                .as_ref()
                .expect("only actual Root admission can refuse")
                .clone();
            let changed = memory.changed_since(memory.snapshot());
            tokio::pin!(changed);
            let ready = std::future::poll_fn(|cx| {
                use std::future::Future;
                std::task::Poll::Ready(changed.as_mut().poll(cx).is_ready())
            })
            .await;
            if let Some(held) = self.packets.current_mut(self.next_emit) {
                if held.prepare(Some(&memory), &self.policy).is_ok() {
                    self.output_pressure = None;
                    return;
                }
            }
            if !ready {
                changed.await;
            }
            return;
        }
        if let Some(deadline) = self.next_deadline() {
            self.packets.wait_capacity(deadline.into()).await;
            self.flush_expired(Instant::now());
        } else {
            self.packets.release_all();
        }
    }
    fn cache(&mut self, dispatch: &InboundUdpDispatch, decision: Decision) {
        if self.decisions.len() >= MAX_DECISIONS {
            return;
        }
        let Ok(key) = FlowKey::try_from_dispatch(dispatch, self.memory.as_ref()) else {
            return;
        };
        let _ = self.decisions.try_push(
            DecisionRow {
                key,
                value: decision,
            },
            MAX_DECISIONS,
        );
    }
    fn resolve(&mut self, index: usize, decision: Decision) {
        let row = self.pending.remove(index);
        self.packets.release(row.value.id, |mut held| {
            held.decision = decision;
            held
        });
        let _ = self.decisions.try_push(
            DecisionRow {
                key: row.key,
                value: decision,
            },
            MAX_DECISIONS,
        );
    }
}
#[cfg(test)]
#[path = "udp/tests.rs"]
mod tests;
