//! Pure original fault custody in the existing finite task rows.
use super::*;
use crate::contract::{
    lifecycle::{CloseReport, TaskFailure},
    scope::TaskJoined,
};
impl Users {
    pub(in crate::controlled_listener::accepted) fn has_task_failure_receiver(&self) -> bool {
        self.task_failures.get().is_some()
    }
    pub(super) fn retain_ordinary_caller_failure(
        &self,
        id: TaskId,
        witness: &TaskJoinWitness,
        joined: &TaskJoined,
        source: &Arc<ChannelError>,
    ) -> bool {
        {
            let mut state = self.state();
            let Some(node) = state.tasks.get_mut(&id) else {
                return false;
            };
            if !node.witness.same_registration(witness)
                || !node.witness.matches_joined(joined)
                || node
                    .received_failure
                    .as_ref()
                    .is_some_and(|old| !node.witness.matches_joined(old))
            {
                return false;
            }
            node.received_failure.get_or_insert_with(|| joined.clone());
        }
        let fault = self
            .state()
            .tasks
            .get(&id)
            .and_then(|node| node.fault.clone());
        match fault {
            Some(fault) => {
                if !fault.matches(witness) || fault.receive(joined).is_err() {
                    return false;
                }
            }
            None if self.task_failures.get().is_some() => return false,
            None => {} // Explicit independent library owner, not Kernel fallback.
        }
        // This actual Source's same paid receiver now holds the exact Arc.
        // No graph/host lock surrounds acceptance; no ACK/native closure.
        witness
            .retain_ordinary_failure(&self.authority.scope, joined, source)
            .is_ok()
    }

    pub(in crate::controlled_listener::accepted) fn has_received_failures(&self) -> bool {
        self.state()
            .tasks
            .values()
            .any(|node| node.received_failure.is_some())
    }
    pub(in crate::controlled_listener::accepted) fn take_received_failures(&self) -> CloseReport {
        let mut report = CloseReport::default();
        loop {
            let node = {
                let mut state = self.state();
                let next = state
                    .tasks
                    .iter()
                    .find_map(|(id, node)| node.retired_failure().then_some(*id));
                next.and_then(|id| state.tasks.remove(&id))
            };
            let Some(node) = node else { break };
            let joined = node
                .received_failure
                .as_ref()
                .expect("retained real failed join");
            let source = joined.result().expect_err("received failed caller");
            report.failed_tasks.push(TaskFailure {
                id: joined.id(),
                class: joined.class(),
                error: ChannelError::retained(source),
            });
            // No last original error/metadata destructor under the Users lock.
            drop(node);
        }
        report
    }
}

impl AcceptedPeerScope {
    /// The original Source's actual already-registered primary liability.
    /// No new actor/slot, Scope, IO grant or join/Host ACK is manufactured.
    pub fn bind_task_failure_registration(
        &self,
        original: &AcceptedTaskFailureRegistration,
    ) -> Result<(), ChannelError> {
        let (id, witness) = self.tasks().current_task().ok_or_else(|| {
            failure(
                Stage::Prepare,
                Reason::Revoked,
                "actual peer primary was not registered",
            )
        })?;
        if !original.matches(&witness)
            || !original
                .receiver()
                .scope_identity()
                .same_scope(&self.users.authority.scope)
            || witness.is_joined()
        {
            return Err(failure(
                Stage::Prepare,
                Reason::StalePlan,
                "peer failure receiver differs from actual registration",
            ));
        }
        if let Some(actual) = self.users.task_failures.get() {
            if !actual.same_receiver(original.receiver()) {
                return Err(failure(
                    Stage::Prepare,
                    Reason::StalePlan,
                    "peer failure receiver changed",
                ));
            }
        } else if self
            .users
            .task_failures
            .set(original.receiver().clone())
            .is_err()
        {
            return Err(failure(
                Stage::Prepare,
                Reason::StalePlan,
                "peer failure receiver raced",
            ));
        }
        let mut state = self.users.state();
        let node = state.tasks.get_mut(&id).ok_or_else(|| {
            failure(
                Stage::Prepare,
                Reason::Revoked,
                "actual peer primary row was removed",
            )
        })?;
        if !node.witness.same_registration(&witness) || node.received_failure.is_some() {
            return Err(failure(
                Stage::Prepare,
                Reason::StalePlan,
                "peer primary fault row changed",
            ));
        }
        if let Some(actual) = &node.fault {
            return if actual.matches(&witness)
                && actual.receiver().same_receiver(original.receiver())
            {
                Ok(())
            } else {
                Err(failure(
                    Stage::Prepare,
                    Reason::StalePlan,
                    "peer primary liability changed",
                ))
            };
        }
        node.fault = Some(original.clone());
        Ok(())
    }
}
