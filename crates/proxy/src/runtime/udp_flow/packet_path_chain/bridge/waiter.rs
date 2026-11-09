//! Finite response custody of the original association; no per-packet task.
use crate::protocol_registry::packet_memory::business::BusinessMemory;
use crate::runtime::udp_flow::response_join::PacketReplySender;
use std::sync::Mutex;
use zero_core::Address;
use zero_engine::EngineError;
use zero_platform_tokio::{
    contract::component::PacketMetadataVec, controlled_carrier::CarrierBufferCredit,
};
use zero_traits::packet_metadata::PacketMetadataArc;

pub(in crate::runtime::udp_flow::packet_path_chain) struct Waiter {
    id: u64,
    target: Address,
    port: u16,
    pub(super) reply: PacketReplySender,
    pub(super) session: u64,
    _address_credit: CarrierBufferCredit,
}
#[derive(Default)]
struct Rows {
    items: PacketMetadataVec<Waiter>,
    next: u64,
}
pub(in crate::runtime::udp_flow::packet_path_chain) struct Waiters {
    rows: Mutex<Rows>,
    memory: BusinessMemory,
}
pub(super) struct Ticket<'a> {
    owner: &'a Waiters,
    id: u64,
    committed: bool,
}
impl Waiters {
    pub(in crate::runtime::udp_flow::packet_path_chain) fn new(
        memory: &BusinessMemory,
    ) -> Result<PacketMetadataArc<Self>, EngineError> {
        let credit = memory
            .metadata
            .reserve(PacketMetadataArc::<Self>::layout().map_err(std::io::Error::other)?)
            .map_err(std::io::Error::other)?;
        PacketMetadataArc::from_prepaid(
            Self {
                rows: Mutex::default(),
                memory: memory.clone(),
            },
            credit,
        )
        .map_err(|(source, _, _)| std::io::Error::other(source).into())
    }
    pub(super) fn insert(
        &self,
        target: &Address,
        port: u16,
        reply: PacketReplySender,
        session: u64,
    ) -> Result<Ticket<'_>, EngineError> {
        let size = match target {
            Address::Domain(name) => name.len(),
            _ => 0,
        };
        let credit = self.memory.bytes.try_reserve(size)?;
        let mut waiter = Waiter {
            id: 0,
            target: target.clone(),
            port,
            reply,
            session,
            _address_credit: credit,
        };
        let notifications = self.memory.metadata.notifications();
        let mut rows = self
            .rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = (|| {
            let next = rows
                .next
                .checked_add(1)
                .ok_or_else(|| std::io::Error::other("packet waiter identity exhausted"))?;
            rows.items
                .try_reserve_one_with_metadata(
                    &self.memory.metadata,
                    self.memory.bytes.limits().max_queue_packets,
                )
                .map_err(std::io::Error::other)?;
            rows.next = next;
            Ok::<_, std::io::Error>(next)
        })();
        match result {
            Ok(id) => {
                waiter.id = id;
                let result = rows.items.push_reserved(waiter);
                drop(rows);
                drop(notifications);
                if let Err(actual) = result {
                    drop(actual);
                    return Err(std::io::Error::other("prepaid packet waiter slot was lost").into());
                }
                Ok(Ticket {
                    owner: self,
                    id,
                    committed: false,
                })
            }
            Err(source) => {
                drop(rows);
                drop(notifications);
                drop(waiter);
                Err(source.into())
            }
        }
    }
    pub(super) fn take_matching(&self, target: &Address, port: u16) -> Option<Waiter> {
        let mut rows = self
            .rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let index = rows
            .items
            .iter()
            .position(|row| row.target == *target && row.port == port)?;
        Some(rows.items.remove(index))
    }
    fn remove_id(&self, id: u64) -> Option<Waiter> {
        let mut rows = self
            .rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let index = rows.items.iter().position(|row| row.id == id)?;
        Some(rows.items.remove(index))
    }
}
impl Ticket<'_> {
    pub(super) fn commit(mut self) {
        self.committed = true;
    }
}
impl Drop for Ticket<'_> {
    fn drop(&mut self) {
        if !self.committed {
            drop(self.owner.remove_id(self.id));
        }
    }
}
