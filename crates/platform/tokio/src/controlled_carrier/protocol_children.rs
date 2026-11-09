//! Child protocol originals in the real managed physical record, not a Query
//! impersonation or another cleanup executor. The original Root drives close.
use crate::{
    contract::{
        component::{PacketDriverMetadataView, PacketMetadataVec},
        ChannelError, Impact, Reason, Stage,
    },
    controlled_carrier::ProtocolOriginal,
};
use std::sync::Mutex;
use tokio::time::Instant;
use zero_traits::packet_metadata::PacketMetadataArc;
mod failure;
use failure::Record;
pub struct ProtocolOriginalChildren {
    records: Mutex<PacketMetadataVec<PacketMetadataArc<Record>>>,
    memory: PacketDriverMetadataView,
    limit: usize,
    next: std::sync::atomic::AtomicU64,
}
fn capacity(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    ChannelError::new(Stage::Prepare, Reason::Capacity, Impact::Component).with_source(source)
}
impl ProtocolOriginalChildren {
    pub fn new(memory: PacketDriverMetadataView, limit: usize) -> Self {
        Self {
            records: Mutex::new(Default::default()),
            memory,
            limit,
            next: std::sync::atomic::AtomicU64::new(0),
        }
    }
    /// Caller/anchor/actual current graph must be validated by this concrete
    /// Pending receiver before reaching its own original record.
    pub fn retain(&self, original: ProtocolOriginal) -> Result<(), ChannelError> {
        self.retain_for(original, None)
    }
    /// Same actual ingress/diagnostic witness, not a Scope-only synthetic key.
    pub fn retain_for_caller(
        &self,
        original: ProtocolOriginal,
        caller: crate::contract::scope::TaskJoinWitness,
    ) -> Result<(), ChannelError> {
        self.retain_for(original, Some(caller))
    }
    fn retain_for(
        &self,
        original: ProtocolOriginal,
        caller: Option<crate::contract::scope::TaskJoinWitness>,
    ) -> Result<(), ChannelError> {
        self.reclaim_returned();
        if self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .any(|record| {
                record.original.same_original(&original)
                    && same_caller(record.caller.as_ref(), caller.as_ref())
            })
        {
            return Ok(());
        }
        let ordinal = self
            .next
            .fetch_update(
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
                |current| current.checked_add(1),
            )
            .map_err(|_| ChannelError::new(Stage::Prepare, Reason::Capacity, Impact::Component))?;
        let mut prepared = Some(Record::prepare(
            original,
            &self.memory,
            ordinal,
            caller.clone(),
        )?);
        let notifications = self.memory.notifications();
        let mut rejected = None;
        let result = {
            let mut records = self
                .records
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if records.iter().any(|record| {
                record
                    .original
                    .same_original(&prepared.as_ref().unwrap().original)
                    && same_caller(record.caller.as_ref(), caller.as_ref())
            }) {
                Ok(())
            } else {
                records
                    .try_reserve_one_with_metadata(&self.memory, self.limit)
                    .map_err(capacity)
                    .and_then(|()| {
                        records
                            .push_reserved(prepared.take().unwrap())
                            .map_err(|record| {
                                rejected = Some(record);
                                ChannelError::new(
                                    Stage::Prepare,
                                    Reason::Capacity,
                                    Impact::Component,
                                )
                            })
                    })
            }
        };
        drop(notifications);
        drop(prepared);
        drop(rejected);
        result
    }
    pub fn reclaim_returned(&self) {
        loop {
            let removed = {
                let mut records = self
                    .records
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let index = records
                    .iter()
                    .position(|record| record.original.is_returned());
                index.map(|index| records.remove(index))
            };
            match removed {
                Some(record) => drop(record),
                None => break,
            }
        }
    }
    pub fn returned(&self) -> bool {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .all(|record| record.original.is_returned())
    }
    pub fn metadata_view(&self) -> &PacketDriverMetadataView {
        &self.memory
    }
    pub fn same_memory(&self, memory: &PacketDriverMetadataView) -> bool {
        self.memory.same_root(memory)
    }
    pub fn has_caller(&self, caller: &crate::contract::scope::TaskJoinWitness) -> bool {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .any(|record| {
                same_caller(record.caller.as_ref(), Some(caller)) && !record.original.is_returned()
            })
    }
    pub fn request_caller_return(&self, caller: &crate::contract::scope::TaskJoinWitness) {
        let mut after = None;
        while let Some(record) = self.next_record(after) {
            if same_caller(record.caller.as_ref(), Some(caller)) {
                record.original.request_retirement();
            }
            after = Some(record.ordinal);
        }
    }
    pub async fn wait_caller_returned(
        &self,
        caller: &crate::contract::scope::TaskJoinWitness,
        end: Instant,
    ) -> Result<bool, ChannelError> {
        self.request_caller_return(caller);
        let work = async {
            let mut after = None;
            while let Some(record) = self.next_record(after) {
                if same_caller(record.caller.as_ref(), Some(caller)) {
                    if let Err(source) = record.original.wait_returned_original().await {
                        let (retained, repeated) = record.receive(source);
                        drop(repeated);
                        return Err(ChannelError::retained(retained));
                    }
                }
                after = Some(record.ordinal);
            }
            Ok(())
        };
        let complete = match tokio::time::timeout_at(end, work).await {
            Ok(result) => result.map(|()| true),
            Err(_) => Ok(false),
        };
        self.reclaim_returned();
        complete
    }
    /// Request ALL outstanding actual originals before waiting on any one.
    /// This avoids a late independent child waiting for the first child's ACK.
    fn next_record(&self, after: Option<u64>) -> Option<PacketMetadataArc<Record>> {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|record| after.is_none_or(|after| record.ordinal > after))
            .min_by_key(|record| record.ordinal)
            .cloned()
    }
    /// Publish only after the original physical driver is actually installed.
    /// Both supplied receipts are observed from that same physical adoption.
    pub fn receive_driver(
        &self,
        original: &crate::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        let Some(loan) = original.retained_loan() else {
            return Ok(());
        };
        let keeper = original.retained_driver().ok_or_else(|| {
            ChannelError::new(Stage::Prepare, Reason::StalePlan, Impact::Component)
        })?;
        let mut after = None;
        while let Some(record) = self.next_record(after) {
            if !record.original.is_returned() && loan.retains_protocol_original(&record.original) {
                record
                    .original
                    .prepare_loan_receiver(loan.clone(), keeper.clone())?
                    .confirm()?;
            }
            after = Some(record.ordinal);
        }
        self.reclaim_returned();
        Ok(())
    }
    pub fn request_return(&self) {
        let mut after = None;
        while let Some(record) = self.next_record(after) {
            record.original.request_retirement();
            after = Some(record.ordinal);
        }
    }
    /// Call only after returning the upper LoanOwner's actual IO/views and
    /// consuming its real setup body. No new child can enter that old caller.
    /// A cached child's sealed receiver handoff preserves its own physical N;
    /// an unhanded child still requires true global original settlement.
    pub async fn wait_returned(&self, end: Instant) -> Result<bool, ChannelError> {
        match tokio::time::timeout_at(end, self.wait_returned_original()).await {
            Ok(result) => result.map(|()| true),
            Err(_) => Ok(false),
        }
    }
    /// Only the real Root/holder runs this wait. No observer timer is installed
    /// in the cleanup; losing an observer cannot abandon any retained child.
    pub async fn wait_returned_original(&self) -> Result<(), ChannelError> {
        self.request_return();
        let mut after = None;
        while let Some(record) = self.next_record(after) {
            if let Err(source) = record.original.wait_returned_original().await {
                let (retained, repeated) = record.receive(source);
                drop(repeated);
                return Err(ChannelError::retained(retained));
            }
            after = Some(record.ordinal);
        }
        self.reclaim_returned();
        Ok(())
    }
}

fn same_caller(
    left: Option<&crate::contract::scope::TaskJoinWitness>,
    right: Option<&crate::contract::scope::TaskJoinWitness>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => left.same_registration(right),
        _ => false,
    }
}
