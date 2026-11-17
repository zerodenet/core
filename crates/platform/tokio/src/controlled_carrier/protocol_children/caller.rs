//! Observation of a real initializer already retained by this exact parent.
use super::*;
use crate::contract::scope::TaskJoinWitness;
impl ProtocolOriginalChildren {
    /// Return only the current native task's original initializer witness from
    /// an already retained child. This grants no task, I/O or close authority.
    pub fn current_initializer(&self) -> Option<TaskJoinWitness> {
        let native = tokio::task::try_id()?;
        let mut after = None;
        while let Some(record) = self.next_record(after) {
            after = Some(record.ordinal);
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
