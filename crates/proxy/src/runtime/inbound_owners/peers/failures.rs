//! Prepay the actual primary before its factory or registrar can run.
use super::*;
use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};
impl InboundPeerTasks {
    pub(in crate::runtime::inbound_owners) fn bind_task_failures(
        &mut self,
        receiver: zero_platform_tokio::AcceptedTaskFailures,
    ) -> Result<(), ChannelError> {
        if receiver.instance() != self.identity || self.client_task_failures.is_some() {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                Impact::Component,
            ));
        }
        self.client_task_failures = Some(receiver);
        Ok(())
    }
    pub(in crate::runtime::inbound_owners) fn reserve_task_failure(
        &self,
        host: &InboundTaskAccess,
    ) -> Result<Option<zero_platform_tokio::AcceptedTaskFailureReservation>, EngineError> {
        let actual = self.client_task_failures.as_ref().ok_or_else(|| {
            std::io::Error::other("actual peer Source has no paid task-fault receiver")
        })?;
        if !actual
            .scope_identity()
            .same_scope(&host.peer_registrar().scope_identity())
        {
            return Err(
                std::io::Error::other("peer fault receiver belongs to another Scope").into(),
            );
        }
        match actual.reserve() {
            Ok(reserved) => Ok(Some(reserved)),
            // Same existing bounded peer admission: no work/native factory has
            // run, and no active peer or valid fault history is evicted.
            Err(error) if error.reason == Reason::Capacity => Ok(None),
            Err(error) => Err(std::io::Error::other(error).into()),
        }
    }
    pub(in crate::runtime::inbound_owners) fn original_caller_witness(
        &self,
        task: TaskId,
    ) -> Option<zero_platform_tokio::contract::scope::TaskJoinWitness> {
        self.state.lock().unwrap().active.get(&task)?.join_witness()
    }
}
