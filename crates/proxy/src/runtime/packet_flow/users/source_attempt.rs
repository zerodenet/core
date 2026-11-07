//! A real Source's retained attempt owns cleanup, not a fabricated caller join.
use super::*;
use std::sync::{atomic::Ordering, Weak};
pub(crate) struct FlowLogicalClose {
    owner: Weak<FlowUsers>,
    record: Weak<Record>,
    logical: Arc<
        Mutex<
            Option<Arc<zero_platform_tokio::controlled_carrier::loan::LogicalLoanCloseParticipant>>,
        >,
    >,
}
impl FlowLogicalClose {
    /// Called only after this same actual virtual controller reports closure.
    /// This never returns the original Source task/native/whole Scope credit.
    pub(crate) fn confirm(&self) {
        if let Some(record) = self.record.upgrade() {
            if let Some(closed) = &record.logical_closed {
                closed.store(true, Ordering::Release);
            }
        }
        if let Some(actual) = self.publisher() {
            actual.confirm();
        }
        if let Some(owner) = self.owner.upgrade() {
            owner.changed.notify_waiters();
        }
    }
    fn publisher(
        &self,
    ) -> Option<Arc<zero_platform_tokio::controlled_carrier::loan::LogicalLoanCloseParticipant>>
    {
        self.logical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    pub(crate) fn continuation_started(&self) {
        if let Some(actual) = self.publisher() {
            actual.continuation_started();
        }
    }
    pub(crate) fn deadline_failure(&self, source: tokio::time::error::Elapsed) -> ChannelError {
        match self.publisher() {
            Some(actual) => actual.deadline(source),
            None => ChannelError::new(Stage::Close, Reason::Timeout, Impact::Session)
                .with_source(source),
        }
    }
    pub(crate) fn terminal_failure(&self, source: Arc<ChannelError>) -> ChannelError {
        match self.publisher() {
            Some(actual) => actual.terminal(source),
            None => ChannelError::retained(source),
        }
    }
}
impl FlowUser {
    pub(crate) fn logical_completion(&self) -> FlowLogicalClose {
        FlowLogicalClose {
            owner: Arc::downgrade(&self.owner),
            record: Arc::downgrade(&self.record),
            logical: self.logical_publisher.clone(),
        }
    }
    pub(crate) fn mark_logical_closed(&self) {
        self.logical_completion().confirm();
    }
}
impl FlowUsers {
    pub(crate) fn acquire_from_packet_call(
        self: &Arc<Self>,
        witness: TaskJoinWitness,
        custody: Option<zero_platform_tokio::contract::component::PacketDriverCallRetained>,
    ) -> Result<FlowUser, ChannelError> {
        let Some(custody) = custody else {
            return self.acquire_from_witness(witness);
        };
        if witness.is_joined()
            || !witness.scope_identity().resource_open()
            || (!custody.retained() || !custody.matches_caller(&witness))
        {
            return Err(revoked());
        }
        self.acquire_with_completion(Some(FlowCaller { witness }), None, Some(custody))
    }
}
