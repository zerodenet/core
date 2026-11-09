use super::super::borrowed::sniff_tls_name;
use super::*;
use zero_transport::quic_initial::{looks_like_client_initial, QuicInitialObservation as Outcome};
impl TunQuicSniffer {
    pub(super) async fn observe(&mut self, datagram: TunDatagram) {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        let destination = datagram.destination;
        if let Some(row) = self
            .decisions
            .iter()
            .find(|row| row.destination == destination)
        {
            let name = match row.value {
                Decision::Domain(name) => Some(name),
                Decision::Fallback => None,
            };
            let ready = Held::new(datagram, name);
            self.push(sequence, None, ready);
            return;
        }
        let mut pending = self
            .pending
            .iter()
            .position(|pending| pending.destination == destination);
        if pending.is_none()
            && (!quic_sniff_port(destination.port) || !looks_like_client_initial(&datagram.payload))
        {
            self.push(sequence, None, Held::new(datagram, None));
            return;
        }
        if pending.is_none() {
            if self.pending.len() >= MAX_PENDING_DESTINATIONS {
                self.push(sequence, None, Held::new(datagram, None));
                return;
            }
            let deadline = Instant::now() + QUIC_SNIFF_TIMEOUT;
            let sniffer = match &self.memory {
                Some(memory) => {
                    match QuicInitialSniffer::prepare_metadata(memory.clone(), deadline.into())
                        .await
                    {
                        Ok(sniffer) => sniffer,
                        Err(_) => {
                            self.push(sequence, None, Held::new(datagram, None));
                            return;
                        }
                    }
                }
                None => QuicInitialSniffer::new(),
            };
            if self
                .pending
                .try_push(
                    PendingQuic {
                        destination,
                        id: sequence,
                        sniffer,
                        datagrams: 0,
                        buffered_bytes: 0,
                        deadline,
                    },
                    MAX_PENDING_DESTINATIONS,
                )
                .is_err()
            {
                self.push(sequence, None, Held::new(datagram, None));
                return;
            }
            pending = Some(self.pending.len() - 1);
        }
        let index = pending.expect("same actual pending TUN row");
        let (id, decision) = {
            let pending = &mut self.pending[index];
            pending.datagrams += 1;
            pending.buffered_bytes = pending
                .buffered_bytes
                .saturating_add(datagram.payload.len());
            let outcome = if self.memory.is_some() {
                pending
                    .sniffer
                    .observe_prepaid_until(&datagram.payload, pending.deadline.into())
                    .await
            } else {
                pending.sniffer.observe(&datagram.payload)
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
                        let decision =
                            sniff_tls_name(&hello).map_or(Decision::Fallback, Decision::Domain);
                        drop(hello);
                        Some(decision)
                    }
                    Outcome::NotInitial | Outcome::Rejected => Some(Decision::Fallback),
                }
            };
            (pending.id, decision)
        };
        self.push(sequence, Some(id), Held::new(datagram, None));
        if self
            .pending
            .get(index)
            .is_some_and(|pending| pending.id == id)
        {
            if let Some(decision) = decision {
                self.resolve(index, decision);
            }
        }
    }
}
