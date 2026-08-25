use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use zero_core::{Address, TargetHostSource};
use zero_traits::SocketAddress;
use zero_transport::quic_initial::{
    looks_like_client_initial, QuicInitialOutcome, QuicInitialSniffer,
};

use super::{normalize_sniffed_domain, sniff_tls_handshake, SniffOutcome};
use crate::inbound::tun::udp::TunDatagram;

const QUIC_SNIFF_TIMEOUT: Duration = Duration::from_millis(200);
const MAX_PENDING_DESTINATIONS: usize = 32;
const MAX_DECIDED_DESTINATIONS: usize = 64;
const MAX_BUFFERED_DATAGRAMS: usize = 8;
const MAX_BUFFERED_BYTES: usize = 64 * 1024;

pub(super) struct SniffedTunDatagram {
    pub(super) original_destination: SocketAddress,
    pub(super) target: Address,
    pub(super) payload: Vec<u8>,
    pub(super) host_source: Option<TargetHostSource>,
}

struct PendingQuic {
    sniffer: QuicInitialSniffer,
    datagrams: VecDeque<TunDatagram>,
    buffered_bytes: usize,
    deadline: Instant,
}

#[derive(Clone)]
enum Decision {
    Domain(String),
    Fallback,
}

#[derive(Default)]
pub(super) struct TunQuicSniffer {
    pending: HashMap<SocketAddress, PendingQuic>,
    decisions: HashMap<SocketAddress, Decision>,
    ready: VecDeque<SniffedTunDatagram>,
}

impl TunQuicSniffer {
    pub(super) async fn next(
        &mut self,
        receiver: &mut mpsc::Receiver<TunDatagram>,
    ) -> Option<SniffedTunDatagram> {
        loop {
            if let Some(datagram) = self.ready.pop_front() {
                return Some(datagram);
            }

            let received = if let Some(deadline) = self.next_deadline() {
                match tokio::time::timeout_at(deadline.into(), receiver.recv()).await {
                    Ok(received) => received,
                    Err(_) => {
                        self.flush_expired(Instant::now());
                        continue;
                    }
                }
            } else {
                receiver.recv().await
            };
            let datagram = received?;
            self.observe(datagram);
        }
    }

    fn observe(&mut self, datagram: TunDatagram) {
        let destination = datagram.destination;
        if let Some(decision) = self.decisions.get(&destination).cloned() {
            self.ready.push_back(apply_decision(datagram, decision));
            return;
        }
        if !self.pending.contains_key(&destination)
            && (!quic_sniff_port(destination.port)
                || !looks_like_client_initial(&datagram.payload))
        {
            self.ready
                .push_back(apply_decision(datagram, Decision::Fallback));
            return;
        }
        if !self.pending.contains_key(&destination)
            && self.pending.len() >= MAX_PENDING_DESTINATIONS
        {
            self.ready
                .push_back(apply_decision(datagram, Decision::Fallback));
            return;
        }

        let pending = self.pending.entry(destination).or_insert_with(|| PendingQuic {
            sniffer: QuicInitialSniffer::new(),
            datagrams: VecDeque::new(),
            buffered_bytes: 0,
            deadline: Instant::now() + QUIC_SNIFF_TIMEOUT,
        });
        pending.buffered_bytes = pending.buffered_bytes.saturating_add(datagram.payload.len());
        let outcome = pending.sniffer.ingest(&datagram.payload);
        pending.datagrams.push_back(datagram);
        if pending.datagrams.len() > MAX_BUFFERED_DATAGRAMS
            || pending.buffered_bytes > MAX_BUFFERED_BYTES
        {
            self.resolve(destination, Decision::Fallback);
            return;
        }
        match outcome {
            QuicInitialOutcome::Pending => {}
            QuicInitialOutcome::ClientHello(client_hello) => {
                let decision = match sniff_tls_handshake(&client_hello) {
                    Ok(SniffOutcome::Domain { domain, .. }) => normalize_sniffed_domain(domain)
                        .map_or(Decision::Fallback, Decision::Domain),
                    Ok(SniffOutcome::EncryptedClientHello | SniffOutcome::None) | Err(_) => {
                        Decision::Fallback
                    }
                };
                self.resolve(destination, decision);
            }
            QuicInitialOutcome::NotInitial | QuicInitialOutcome::Rejected => {
                self.resolve(destination, Decision::Fallback);
            }
        }
    }

    fn resolve(&mut self, destination: SocketAddress, decision: Decision) {
        let Some(mut pending) = self.pending.remove(&destination) else {
            return;
        };
        if self.decisions.len() < MAX_DECIDED_DESTINATIONS {
            self.decisions.insert(destination, decision.clone());
        }
        while let Some(datagram) = pending.datagrams.pop_front() {
            self.ready.push_back(apply_decision(datagram, decision.clone()));
        }
    }

    fn flush_expired(&mut self, now: Instant) {
        let expired = self
            .pending
            .iter()
            .filter_map(|(destination, pending)| (pending.deadline <= now).then_some(*destination))
            .collect::<Vec<_>>();
        for destination in expired {
            self.resolve(destination, Decision::Fallback);
        }
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.pending.values().map(|pending| pending.deadline).min()
    }
}

fn quic_sniff_port(port: u16) -> bool {
    matches!(port, 443 | 8443)
}

fn apply_decision(datagram: TunDatagram, decision: Decision) -> SniffedTunDatagram {
    let original_destination = datagram.destination;
    match decision {
        Decision::Domain(domain) => SniffedTunDatagram {
            original_destination,
            target: Address::Domain(domain),
            payload: datagram.payload,
            host_source: Some(TargetHostSource::QuicSni),
        },
        Decision::Fallback => SniffedTunDatagram {
            original_destination,
            target: socket_address_to_address(original_destination),
            payload: datagram.payload,
            host_source: None,
        },
    }
}

fn socket_address_to_address(address: SocketAddress) -> Address {
    match address.ip {
        zero_traits::IpAddress::V4(ip) => Address::Ipv4(ip),
        zero_traits::IpAddress::V6(ip) => Address::Ipv6(ip),
    }
}

#[cfg(test)]
mod tests;
