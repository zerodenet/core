//! The same original record map, without Scope/registrar or body ownership.
use super::super::*;
use crate::contract::scope::TaskScopeIdentity;
trait OriginalRegistrationCensus: Send + Sync {
    fn next(&self, after: Option<TaskId>) -> Option<(TaskId, TaskJoinWitness)>;
    fn remaining(&self, after: Option<TaskId>) -> usize;
}
impl<O: DriverKind, B: Send + Sync + 'static> OriginalRegistrationCensus for Mutex<State<O, B>> {
    fn next(&self, after: Option<TaskId>) -> Option<(TaskId, TaskJoinWitness)> {
        use std::ops::Bound::{Excluded, Unbounded};
        let state = self.lock().unwrap();
        let (_, record) = state
            .records
            .range((after.map_or(Unbounded, Excluded), Unbounded))
            .next()?;
        let registration = record.registration.lock().unwrap();
        registration
            .as_ref()
            .map(|(id, witness)| (*id, witness.clone()))
    }
    fn remaining(&self, after: Option<TaskId>) -> usize {
        use std::ops::Bound::{Excluded, Unbounded};
        self.lock()
            .unwrap()
            .records
            .range((after.map_or(Unbounded, Excluded), Unbounded))
            .count()
    }
}
/// Kernel completion shares the original sealed ledger only after all real
/// joins/deliveries and body transfers. The independent library constructor
/// retains its original snapshot semantics. Neither grants task/native rights.
#[derive(Clone)]
pub struct DriverRegistrationsClosed {
    pub(in crate::controlled_datagram::metadata::users) scope: TaskScopeIdentity,
    storage: Storage,
}
#[derive(Clone)]
enum Storage {
    Library(Arc<[TaskJoinWitness]>),
    Original(Arc<dyn OriginalRegistrationCensus>),
}
impl DriverRegistrationsClosed {
    pub(super) fn library(scope: TaskScopeIdentity, registrations: Arc<[TaskJoinWitness]>) -> Self {
        Self {
            scope,
            storage: Storage::Library(registrations),
        }
    }
    /// Cursor over this original sealed ledger; no snapshot allocation or ACK.
    pub fn registrations(&self) -> super::iteration::RegistrationIter {
        super::iteration::RegistrationSequence::Original(self.clone()).iter()
    }
    pub(super) fn next(&self, after: Option<TaskId>) -> Option<(TaskId, TaskJoinWitness)> {
        match &self.storage {
            Storage::Original(census) => census.next(after),
            Storage::Library(values) => values.iter().find_map(|witness| {
                let id = witness.task_id()?;
                after
                    .is_none_or(|after| id > after)
                    .then(|| (id, witness.clone()))
            }),
        }
    }
    pub(super) fn remaining(&self, after: Option<TaskId>) -> usize {
        match &self.storage {
            Storage::Original(census) => census.remaining(after),
            Storage::Library(values) => values
                .iter()
                .filter(|witness| {
                    witness
                        .task_id()
                        .is_some_and(|id| after.is_none_or(|after| id > after))
                })
                .count(),
        }
    }
}
impl<O: DriverKind, B: Send + Sync + 'static> DriverOwner<O, B> {
    pub fn owned_registration_completion(&self) -> Option<DriverRegistrationsClosed> {
        let scope = self.scope_identity();
        if !self.completion_matches(&scope) {
            return None;
        }
        {
            let state = self.shared.state.lock().unwrap();
            if !state
                .records
                .values()
                .all(|record| record.body.lock().unwrap().returned())
            {
                return None;
            }
        }
        // Unsizing the same Arc<Mutex<State>> allocates no second map or Arc
        // payload. State has no registrar, callback, task future or IO field;
        // all its B fields are permanently Returned, holding weak identity only.
        Some(DriverRegistrationsClosed {
            scope,
            storage: Storage::Original(self.shared.state.clone()),
        })
    }
}
