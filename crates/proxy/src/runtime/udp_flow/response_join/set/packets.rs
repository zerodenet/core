//! Pure bounded output of the same association receiver. No task join is forged.
use super::*;
use crate::protocol_registry::packet_memory::business::BusinessMemory;
use zero_platform_tokio::contract::component::PacketMetadataVec;
use zero_traits::packet_metadata::PacketMetadataArc;

pub(crate) struct PacketReplies {
    memory: BusinessMemory,
    packets: Mutex<PacketMetadataVec<Packet>>,
    closed: AtomicBool,
    relay: Arc<wake::Relay>,
}
#[derive(Clone)]
pub(crate) struct PacketReplySender(PacketMetadataArc<PacketReplies>);
impl PacketReplies {
    fn new(
        memory: &BusinessMemory,
        relay: &Arc<wake::Relay>,
    ) -> Result<PacketMetadataArc<Self>, EngineError> {
        let layout = PacketMetadataArc::<Self>::layout().map_err(std::io::Error::other)?;
        let credit = memory
            .metadata
            .reserve(layout)
            .map_err(std::io::Error::other)?;
        PacketMetadataArc::from_prepaid(
            Self {
                memory: memory.clone(),
                packets: Mutex::default(),
                closed: AtomicBool::new(false),
                relay: relay.clone(),
            },
            credit,
        )
        .map_err(|(source, _, _)| std::io::Error::other(source).into())
    }
    pub(super) fn pop(&self) -> Option<Packet> {
        let mut packets = self
            .packets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if packets.is_empty() {
            None
        } else {
            Some(packets.remove(0))
        }
    }
    pub(super) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
    pub(super) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        // Actual data and its tickets drop after the table lock is released.
        while let Some(packet) = self.pop() {
            drop(packet);
        }
        self.relay.signal();
    }
}
impl PacketReplySender {
    pub(crate) fn send(&self, packet: Packet) -> Result<(), EngineError> {
        let notifications = self.0.memory.metadata.notifications();
        let mut packets = self
            .0
            .packets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = if self.0.closed.load(Ordering::Acquire) {
            Err((
                std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "same response owner is retired",
                ),
                packet,
            ))
        } else {
            packets
                .try_push_with_metadata(
                    packet,
                    &self.0.memory.metadata,
                    self.0.memory.bytes.limits().max_queue_packets,
                )
                .map_err(|(source, packet)| (std::io::Error::other(source), packet))
        };
        drop(packets);
        drop(notifications);
        match result {
            Ok(()) => {
                self.0.relay.signal();
                Ok(())
            }
            Err((source, packet)) => {
                drop(packet);
                Err(source.into())
            }
        }
    }
}
impl ResponseSetLease {
    pub(crate) fn packet_sender(
        &mut self,
        memory: &BusinessMemory,
    ) -> Result<PacketReplySender, EngineError> {
        if self.owner.revoked.load(Ordering::Acquire) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "same response owner is retired",
            )
            .into());
        }
        if self.owner.packets.get().is_none() {
            let actual = PacketReplies::new(memory, &self.owner.wake)?;
            if let Err(unused) = self.owner.packets.set(actual) {
                drop(unused);
            }
        }
        let actual = self
            .owner
            .packets
            .get()
            .expect("same sole response lease initialized its inbox");
        if !actual.memory.same(memory) {
            return Err(std::io::Error::other(
                "response inbox belongs to a different original Host",
            )
            .into());
        }
        Ok(PacketReplySender(actual.clone()))
    }
}

#[cfg(test)]
mod tests;
