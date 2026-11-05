//! One actual authentication's receive queues and fragment payload admission.
use crate::udp::{Hysteria2UdpPacket, Hysteria2UdpReassembler};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(super) struct Packet {
    packet: Hysteria2UdpPacket,
    payload: OwnedSemaphorePermit,
}
impl Packet {
    pub(super) fn reserve(packet: Hysteria2UdpPacket, bytes: &Arc<Semaphore>) -> Option<Self> {
        // Empty datagrams still consume a position. Wire parsing is bounded by
        // one QUIC datagram; retained payload admission happens before enqueue.
        let count = u32::try_from(packet.payload().len().max(1)).ok()?;
        let payload = bytes.clone().try_acquire_many_owned(count).ok()?;
        Some(Self { packet, payload })
    }
}

#[derive(Default)]
pub(super) struct Reassembly {
    inner: Hysteria2UdpReassembler,
    payloads: BTreeMap<(u32, u16), Vec<OwnedSemaphorePermit>>,
}
impl Reassembly {
    pub(super) fn push(&mut self, packet: Packet) -> Option<Hysteria2UdpPacket> {
        let Packet { packet, payload } = packet;
        let key = (packet.session_id(), packet.packet_id());
        if packet.fragment_count() == 1 {
            return Some(packet);
        }
        // Refuse new assemblies at capacity, retaining active incomplete
        // datagrams. Duplicate fragments never consume retained bytes twice.
        if packet.fragment_count() > 64
            || !self.inner.can_accept(&packet)
            || self.inner.has_fragment(&packet)
        {
            return None;
        }
        self.payloads.entry(key).or_default().push(payload);
        let result = self.inner.push(packet).ok().flatten();
        self.payloads.retain(|key, _| self.inner.contains(*key));
        // The consumer now owns a completed payload; it is no longer retained
        // in this authentication's receive queues or incomplete assemblies.
        result
    }
}

#[cfg(test)]
#[path = "buffer/tests.rs"]
mod tests;
