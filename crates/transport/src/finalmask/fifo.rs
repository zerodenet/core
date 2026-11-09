//! The existing per-client tunnel FIFO keeps each original frame and ticket.
use super::{memory::Memory, queue::Packet};
use std::io;
use zero_platform_tokio::contract::component::PacketMetadataVec;
pub(super) struct Fifo {
    values: Storage,
    memory: Memory,
}
enum Storage {
    Library(Vec<Packet>),
    Hosted(PacketMetadataVec<Packet>),
}
impl Fifo {
    pub(super) fn new(memory: Memory) -> Self {
        Self {
            values: if memory.metadata().is_some() {
                Storage::Hosted(Default::default())
            } else {
                Storage::Library(Vec::new())
            },
            memory,
        }
    }
    pub(super) fn push(&mut self, packet: Packet) -> io::Result<()> {
        if self.len() >= 512 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        match &mut self.values {
            Storage::Library(values) => {
                values.try_reserve(1).map_err(io::Error::other)?;
                values.push(packet);
            }
            Storage::Hosted(values) => {
                self.memory.reserve_table(values, 512)?;
                assert!(
                    values.push_reserved(packet).is_ok(),
                    "original FIFO slot prepaid"
                );
            }
        }
        Ok(())
    }
    pub(super) fn pop(&mut self) -> Option<Packet> {
        if self.is_empty() {
            return None;
        }
        Some(match &mut self.values {
            Storage::Library(values) => values.remove(0),
            Storage::Hosted(values) => values.remove(0),
        })
    }
    pub(super) fn front(&self) -> Option<&Packet> {
        self.values().first()
    }
    pub(super) fn len(&self) -> usize {
        self.values().len()
    }
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub(super) fn bytes(&self) -> usize {
        self.values().iter().map(|packet| packet.bytes.len()).sum()
    }
    fn values(&self) -> &[Packet] {
        match &self.values {
            Storage::Library(v) => v,
            Storage::Hosted(v) => v,
        }
    }
}
