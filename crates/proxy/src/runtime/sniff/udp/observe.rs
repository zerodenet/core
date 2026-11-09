use super::*;
use crate::runtime::sniff::{
    inline::{sniff_tls_name, NameProgress},
    SniffedProtocol, SNIFF_TIMEOUT,
};
use zero_transport::quic_initial::{
    looks_like_client_initial, QuicInitialObservation as Outcome, QuicInitialSniffer,
};
impl UdpSniffingState {
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
            crate::runtime::sniff::FakeDnsMetadata::Domain(sniffed) => {
                let decision = InlineName::new(&sniffed.domain)
                    .filter(|name| {
                        self.policy
                            .accepts_name(sniffed.protocol, name.as_str(), false)
                    })
                    .map_or(Decision::Fallback, |name| Decision::Domain {
                        name,
                        protocol: sniffed.protocol,
                        source: sniffed.source,
                        fake_dns_fallback: false,
                    });
                let existing = self
                    .decisions
                    .iter()
                    .position(|row| row.key.matches(&dispatch));
                let key = if existing.is_none() && self.decisions.len() < MAX_DECISIONS {
                    FlowKey::try_from_dispatch(&dispatch, self.memory.as_ref()).ok()
                } else {
                    None
                };
                if let Some(index) = existing {
                    self.decisions[index].value = decision;
                } else if let Some(key) = key {
                    let _ = self.decisions.try_push(
                        DecisionRow {
                            key,
                            value: decision,
                        },
                        MAX_DECISIONS,
                    );
                }
                self.push_decision(sequence, None, dispatch, decision);
                return;
            }
            crate::runtime::sniff::FakeDnsMetadata::ContentFallback => true,
            crate::runtime::sniff::FakeDnsMetadata::None => false,
        };
        if !self.policy.sniffs_quic(fake_dns_fallback) {
            self.push(sequence, None, dispatch);
            return;
        }
        if let Some(row) = self.decisions.iter().find(|row| row.key.matches(&dispatch)) {
            let decision = row.value;
            self.push_decision(sequence, None, dispatch, decision);
            return;
        }
        let mut pending = self
            .pending
            .iter()
            .position(|row| row.key.matches(&dispatch));
        if pending.is_none() && !looks_like_client_initial(dispatch.payload()) {
            self.cache(&dispatch, Decision::Fallback);
            self.push(sequence, None, dispatch);
            return;
        }
        if pending.is_none() {
            if self.pending.len() >= MAX_PENDING_FLOWS {
                self.push(sequence, None, dispatch);
                return;
            }
            let Ok(key) = FlowKey::try_from_dispatch(&dispatch, self.memory.as_ref()) else {
                self.push(sequence, None, dispatch);
                return;
            };
            let deadline = Instant::now() + SNIFF_TIMEOUT;
            let sniffer = match &self.memory {
                Some(memory) => {
                    match QuicInitialSniffer::prepare_metadata(memory.clone(), deadline.into())
                        .await
                    {
                        Ok(sniffer) => sniffer,
                        Err(_) => {
                            self.push(sequence, None, dispatch);
                            return;
                        }
                    }
                }
                None => QuicInitialSniffer::new(),
            };
            let row = PendingRow {
                key,
                value: PendingQuic {
                    id: sequence,
                    sniffer,
                    datagrams: 0,
                    buffered_bytes: 0,
                    deadline,
                    fake_dns_fallback,
                },
            };
            if self.pending.try_push(row, MAX_PENDING_FLOWS).is_err() {
                self.push(sequence, None, dispatch);
                return;
            }
            pending = Some(self.pending.len() - 1);
        }
        let index = pending.expect("same actual pending row");
        let (id, decision) = {
            let pending = &mut self.pending[index].value;
            pending.datagrams += 1;
            pending.buffered_bytes = pending
                .buffered_bytes
                .saturating_add(dispatch.payload().len());
            let outcome = if self.memory.is_some() {
                pending
                    .sniffer
                    .observe_prepaid_until(dispatch.payload(), pending.deadline.into())
                    .await
            } else {
                pending.sniffer.observe(dispatch.payload())
            }
            .unwrap_or(Outcome::Rejected);
            let decision = if pending.datagrams > MAX_BUFFERED_DATAGRAMS
                || pending.buffered_bytes > MAX_BUFFERED_BYTES
            {
                drop(outcome);
                Some(Decision::Fallback)
            } else {
                match outcome {
                    Outcome::Pending => None,
                    Outcome::ClientHello(hello) => {
                        let decision = match sniff_tls_name(&hello) {
                            NameProgress::Domain(name)
                                if self.policy.accepts_name(
                                    SniffedProtocol::Quic,
                                    name.as_str(),
                                    pending.fake_dns_fallback,
                                ) =>
                            {
                                Decision::Domain {
                                    name,
                                    protocol: SniffedProtocol::Quic,
                                    source: TargetHostSource::QuicSni,
                                    fake_dns_fallback: pending.fake_dns_fallback,
                                }
                            }
                            NameProgress::Pending
                            | NameProgress::NoMatch
                            | NameProgress::Domain(_) => Decision::Fallback,
                        };
                        drop(hello);
                        Some(decision)
                    }
                    Outcome::NotInitial | Outcome::Rejected => Some(Decision::Fallback),
                }
            };
            (pending.id, decision)
        };
        self.push(sequence, Some(id), dispatch);
        // The queued input may be inline under row pressure. It is still the
        // same original and participates in this exact flow's resolution.
        if self
            .pending
            .get(index)
            .is_some_and(|row| row.value.id == id)
        {
            if let Some(decision) = decision {
                self.resolve(index, decision);
            }
        }
    }
}
