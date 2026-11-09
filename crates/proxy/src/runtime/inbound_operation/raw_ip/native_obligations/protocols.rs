//! Child originals belong to this same actual raw caller and native record.
use super::*;
use zero_platform_tokio::{
    contract::{component::PacketDriverMetadataView, scope::TaskJoinWitness},
    controlled_carrier::{ProtocolOriginal, ProtocolOriginalChildren},
};
use zero_traits::packet_metadata::PacketMetadataArc;
impl NativeRegistration {
    pub(crate) fn protocol_metadata(&self) -> Result<PacketDriverMetadataView, ChannelError> {
        self.protocol_memory().map(|(memory, _)| memory)
    }
    fn protocol_memory(&self) -> Result<(PacketDriverMetadataView, usize), ChannelError> {
        self.check_open()?;
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| rejected(Reason::Revoked))?;
        let caller = {
            let state = owner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let record = state
                .records
                .get(&self.id)
                .ok_or_else(|| rejected(Reason::Revoked))?;
            if !record.entered || record.retiring || record.kind == ChildKind::Dns {
                return Err(rejected(Reason::Revoked));
            }
            record
                .caller
                .clone()
                .ok_or_else(|| rejected(Reason::Revoked))?
        };
        caller.protocol_metadata()
    }
    pub(crate) fn current_protocol_caller(&self) -> Result<TaskJoinWitness, ChannelError> {
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| rejected(Reason::Revoked))?;
        let (primary, receiver) = {
            let state = owner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let record = state
                .records
                .get(&self.id)
                .ok_or_else(|| rejected(Reason::Revoked))?;
            (
                record.caller.clone(),
                record
                    .receiver
                    .as_ref()
                    .map(|receiver| receiver.witness.clone()),
            )
        };
        primary
            .and_then(|caller| caller.join_witness())
            .into_iter()
            .chain(receiver)
            .find(|witness| {
                !witness.is_joined()
                    && witness.scope_identity().resource_open()
                    && witness
                        .native_task_id()
                        .is_some_and(|id| Some(id) == tokio::task::try_id())
            })
            .ok_or_else(|| rejected(Reason::StalePlan))
    }
    pub(crate) fn observe_protocol_original(
        &self,
        original: ProtocolOriginal,
    ) -> Result<(), ChannelError> {
        let (memory, limit) = self.protocol_memory()?;
        let witness = self.current_protocol_caller()?;
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| rejected(Reason::Revoked))?;
        let existing = {
            let state = owner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state
                .records
                .get(&self.id)
                .and_then(|record| record.protocols.clone())
        };
        let children = match existing {
            Some(children) => children,
            None => {
                let credit = memory
                    .reserve(
                        PacketMetadataArc::<ProtocolOriginalChildren>::layout()
                            .map_err(|source| rejected(Reason::Capacity).with_source(source))?,
                    )
                    .map_err(|source| rejected(Reason::Capacity).with_source(source))?;
                let fresh = PacketMetadataArc::from_prepaid(
                    ProtocolOriginalChildren::new(memory.clone(), limit),
                    credit,
                )
                .map_err(|(source, _, _)| rejected(Reason::Capacity).with_source(source))?;
                let chosen = {
                    let mut state = owner
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if state.closing {
                        return Err(rejected(Reason::Revoked));
                    }
                    let record = state
                        .records
                        .get_mut(&self.id)
                        .ok_or_else(|| rejected(Reason::Revoked))?;
                    if record.retiring {
                        return Err(rejected(Reason::Revoked));
                    }
                    record
                        .protocols
                        .get_or_insert_with(|| fresh.clone())
                        .clone()
                };
                drop(fresh);
                chosen
            }
        };
        if !children.same_memory(&memory) {
            return Err(rejected(Reason::StalePlan));
        }
        children.retain_for_caller(original, witness)
    }
    pub(super) async fn settle_protocols(&self, deadline: Instant) -> Result<bool, ChannelError> {
        let Some(owner) = self.owner.upgrade() else {
            return Ok(false);
        };
        let (children, retiring, caller) = {
            let state = owner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(record) = state.records.get(&self.id) else {
                return Ok(true);
            };
            (
                record.protocols.clone(),
                record.retiring,
                record.caller.clone(),
            )
        };
        let Some(children) = children else {
            return Ok(true);
        };
        if !retiring || caller.and_then(|caller| caller.observed_join()).is_none() {
            return Ok(false);
        }
        children.request_return();
        children.wait_returned(deadline).await
    }
}
