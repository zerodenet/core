//! The existing H1 idle queue, prepaid before its owned backing can grow.
use super::*;
use zero_platform_tokio::contract::component::PacketDriverMetadataView;
use zero_traits::packet_metadata::{PacketMetadataCredit, PacketMetadataLayout};
#[derive(Default)]
pub(super) struct IdleConnections {
    values: Vec<Connection>,
    credit: Option<PacketMetadataCredit>,
    memory: Option<PacketDriverMetadataView>,
}
impl IdleConnections {
    pub(super) fn pop(&mut self) -> Option<Connection> {
        self.values.pop()
    }
    pub(super) fn push(
        &mut self,
        connection: Connection,
        memory: Option<&PacketDriverMetadataView>,
    ) -> Option<Connection> {
        if self.values.len() >= 128 {
            return Some(connection);
        }
        if let Some(memory) = memory {
            if self
                .memory
                .as_ref()
                .is_some_and(|actual| !actual.same_root(memory))
            {
                return Some(connection);
            }
            if self.memory.is_none() {
                // A group belongs to one exact OwnerKey; never import foreign
                // library backing into an installed Host on equal IDs.
                if !self.values.is_empty() {
                    return Some(connection);
                }
                self.memory = Some(memory.clone());
            }
            if self.values.len() == self.values.capacity() {
                let capacity = self.values.capacity().saturating_mul(2).clamp(1, 128);
                if self.reallocate(capacity, memory).is_err() {
                    return Some(connection);
                }
            }
        } else {
            if self.memory.is_some() {
                return Some(connection);
            }
            if self.values.try_reserve(1).is_err() {
                return Some(connection);
            }
        }
        self.values.push(connection);
        None
    }
    fn reallocate(&mut self, capacity: usize, memory: &PacketDriverMetadataView) -> io::Result<()> {
        let credit = memory
            .reserve(PacketMetadataLayout::array::<Connection>(capacity).map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
        let slots = Box::<[Connection]>::new_uninit_slice(capacity);
        let ptr = Box::into_raw(slots) as *mut Connection;
        // SAFETY: exact prepaid allocation, zero initialized elements.
        let mut replacement = unsafe { Vec::from_raw_parts(ptr, 0, capacity) };
        replacement.append(&mut self.values);
        let old = std::mem::replace(&mut self.values, replacement);
        let old_credit = self.credit.replace(credit);
        // Caller holds the same metadata notifications guard outside this
        // mutex. The now-empty old Vec is deallocated before its own refund.
        drop(old);
        drop(old_credit);
        Ok(())
    }
}
