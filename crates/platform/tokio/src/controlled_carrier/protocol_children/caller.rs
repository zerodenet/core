//! Observation of a real initializer already retained by this exact parent.
use super::*;
use crate::contract::scope::TaskJoinWitness;
impl ProtocolOriginalChildren {
    /// Return only the current native task's original initializer witness from
    /// an already retained child. This grants no task, I/O or close authority.
    pub fn current_initializer(&self) -> Option<TaskJoinWitness> {
        self.current_initializer_for(None)
    }
    /// Same original ingress/diagnostic registration, including nested cached
    /// protocol setup. Scope membership alone does not establish this lineage.
    pub fn current_initializer_for_caller(
        &self,
        caller: &TaskJoinWitness,
    ) -> Option<TaskJoinWitness> {
        self.current_initializer_for(Some(caller))
    }
    fn current_initializer_for(
        &self,
        expected: Option<&TaskJoinWitness>,
    ) -> Option<TaskJoinWitness> {
        let native = tokio::task::try_id()?;
        let mut after = None;
        while let Some(record) = self.next_record(after) {
            after = Some(record.ordinal);
            if expected.is_some_and(|expected| {
                record
                    .caller
                    .as_ref()
                    .is_none_or(|actual| !actual.same_registration(expected))
            }) {
                continue;
            }
            if record.original.is_settled() {
                continue;
            }
            let caller = record
                .original
                .adoption()
                .and_then(|original| original.initializer_registration())
                .or_else(|| {
                    record
                        .original
                        .metadata()
                        .and_then(|original| original.initializer_registration())
                });
            if let Some(caller) = caller.filter(|caller| {
                !caller.is_joined()
                    && caller.native_task_id() == Some(native)
                    && caller.scope_identity().resource_open()
                    && caller
                        .scope_identity()
                        .same_scope(record.original.scope_identity())
            }) {
                return Some(caller);
            }
        }
        None
    }
}
