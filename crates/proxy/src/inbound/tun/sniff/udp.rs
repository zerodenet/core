use crate::inbound::tun::udp::TunDatagram;
use crate::runtime::sniff::udp::{queue::Packets, rows::Rows};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use zero_core::{Address, TargetHostSource};
use zero_traits::SocketAddress;
use zero_transport::quic_initial::QuicInitialSniffer;
mod observe;
mod output;
use output::Held;
const QUIC_SNIFF_TIMEOUT: Duration = Duration::from_millis(200);
const MAX_PENDING_DESTINATIONS: usize = 32;
const MAX_DECIDED_DESTINATIONS: usize = 64;
const MAX_BUFFERED_DATAGRAMS: usize = 8;
const MAX_BUFFERED_BYTES: usize = 64 * 1024;
// Existing finite census: each pending destination resolves on its ninth
// original, plus the current receive. This is an array bound, not a new pool.
const MAX_QUEUE_ROWS: usize = MAX_PENDING_DESTINATIONS * (MAX_BUFFERED_DATAGRAMS + 1) + 1;
pub(in crate::inbound::tun) struct SniffedTunDatagram {
    pub(in crate::inbound::tun) peer_identity: Option<std::sync::Arc<str>>,
    pub(in crate::inbound::tun) original_destination: SocketAddress,
    pub(in crate::inbound::tun) target: Address,
    pub(in crate::inbound::tun) payload: zero_traits::PacketBuffer,
    pub(in crate::inbound::tun) host_source: Option<TargetHostSource>,
    // Data is declared first; this exact String ticket travels into Core last.
    pub(in crate::inbound::tun) domain_metadata:
        Option<zero_traits::packet_metadata::PacketMetadataCredit>,
}
struct PendingQuic {
    destination: SocketAddress,
    id: u64,
    sniffer: QuicInitialSniffer,
    datagrams: usize,
    buffered_bytes: usize,
    deadline: Instant,
}
struct Decided {
    destination: SocketAddress,
    value: Decision,
}
#[derive(Clone, Copy)]
enum Decision {
    Domain(crate::runtime::sniff::inline::InlineName),
    Fallback,
}
pub(in crate::inbound::tun) struct TunQuicSniffer {
    memory: Option<zero_platform_tokio::contract::component::PacketDriverMetadataView>,
    pending: Rows<PendingQuic>,
    decisions: Rows<Decided>,
    packets: Packets<Held>,
    next_sequence: u64,
    output_pressure: Option<zero_traits::packet_metadata::PacketMemoryError>,
}
impl TunQuicSniffer {
    pub(in crate::inbound::tun) fn with_metadata(
        memory: zero_platform_tokio::contract::component::PacketDriverMetadataView,
    ) -> Self {
        Self::from_memory(Some(memory))
    }
    fn from_memory(
        memory: Option<zero_platform_tokio::contract::component::PacketDriverMetadataView>,
    ) -> Self {
        Self {
            pending: Rows::new(memory.as_ref()),
            decisions: Rows::new(memory.as_ref()),
            packets: Packets::new(memory.as_ref(), MAX_QUEUE_ROWS),
            memory,
            next_sequence: 0,
            output_pressure: None,
        }
    }
    pub(in crate::inbound::tun) async fn next(
        &mut self,
        receiver: &mut mpsc::Receiver<TunDatagram>,
    ) -> Option<SniffedTunDatagram> {
        loop {
            if let Some(held) = self.packets.ready_mut() {
                match held.prepare(self.memory.as_ref()) {
                    Ok(()) => {
                        self.output_pressure = None;
                        return self.packets.pop_ready().map(Held::into_ready);
                    }
                    Err(source) => {
                        self.output_pressure = Some(source);
                        tracing::trace!(%source,"same TUN known Domain awaits its original Root");
                    }
                }
            }
            if self.output_pressure.is_some() {
                let memory = self
                    .memory
                    .as_ref()
                    .expect("actual Root admission refused")
                    .clone();
                let changed = memory.changed_since(memory.snapshot());
                tokio::pin!(changed);
                let ready = std::future::poll_fn(|cx| {
                    use std::future::Future;
                    std::task::Poll::Ready(changed.as_mut().poll(cx).is_ready())
                })
                .await;
                if let Some(held) = self.packets.ready_mut() {
                    if held.prepare(Some(&memory)).is_ok() {
                        self.output_pressure = None;
                        continue;
                    }
                }
                if !ready {
                    changed.await;
                }
                continue;
            }
            if self.packets.row_pressure() {
                if let Some(deadline) = self.next_deadline() {
                    self.packets.wait_capacity(deadline.into()).await;
                    self.flush_expired(Instant::now());
                } else {
                    self.packets.release_all();
                }
                continue;
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
            match received {
                Some(datagram) => self.observe(datagram).await,
                None => {
                    // No next input can extend a partial Initial. Return every
                    // already admitted original through its existing fallback.
                    self.packets.release_all();
                    self.pending.clear();
                    if self.packets.ready_mut().is_none() {
                        return None;
                    }
                    continue;
                }
            }
        }
    }
    fn push(&mut self, sequence: u64, pending: Option<u64>, datagram: Held) {
        self.packets.push(sequence, pending, datagram);
    }
    fn resolve(&mut self, index: usize, decision: Decision) {
        let pending = self.pending.remove(index);
        self.packets.release(pending.id, |mut actual| {
            actual.name = match decision {
                Decision::Domain(name) => Some(name),
                Decision::Fallback => None,
            };
            actual
        });
        let _ = self.decisions.try_push(
            Decided {
                destination: pending.destination,
                value: decision,
            },
            MAX_DECIDED_DESTINATIONS,
        );
    }
    fn flush_expired(&mut self, now: Instant) {
        while let Some(index) = self
            .pending
            .iter()
            .position(|pending| pending.deadline <= now)
        {
            self.resolve(index, Decision::Fallback);
        }
    }
    fn next_deadline(&self) -> Option<Instant> {
        self.pending.iter().map(|pending| pending.deadline).min()
    }
}
#[cfg(test)]
impl Default for TunQuicSniffer {
    fn default() -> Self {
        Self::from_memory(None)
    }
}
fn quic_sniff_port(port: u16) -> bool {
    matches!(port, 443 | 8443)
}

#[cfg(test)]
mod tests;
