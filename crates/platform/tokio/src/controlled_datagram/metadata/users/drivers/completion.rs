use super::*;
impl<O: DriverKind, B: Send + Sync + 'static> Clone for DriverOwner<O, B> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}
impl<O: DriverKind, B: Send + Sync + 'static> DriverOwner<O, B> {
    pub(in crate::controlled_datagram::metadata::users) fn completion_matches(
        &self,
        scope: &crate::contract::scope::TaskScopeIdentity,
    ) -> bool {
        let state = self.shared.state.lock().unwrap();
        state.retired
            && state.pending == 0
            && !state.count_overflowed
            && scope.same_scope(&self.shared.registrar.scope_identity())
            && state.records.values().all(|record| {
                if !record.delivered.load(Ordering::Acquire) {
                    return false;
                }
                let registration = record.registration.lock().unwrap();
                let received = record.received.lock().unwrap();
                match (registration.as_ref(), received.as_ref()) {
                    (Some((id, witness)), Some(received)) => {
                        *id == received.id()
                            && scope.same_scope(&witness.scope_identity())
                            && witness.matches_joined(received)
                    }
                    _ => false,
                }
            })
    }
}

impl<O: DriverKind, B: Send + Sync + 'static> DriverOwner<O, B> {
    pub(in crate::controlled_datagram::metadata::users) fn completion_registrations(
        &self,
        scope: &crate::contract::scope::TaskScopeIdentity,
    ) -> Option<Arc<[TaskJoinWitness]>> {
        if !self.completion_matches(scope) {
            return None;
        }
        let state = self.shared.state.lock().unwrap();
        // No registration can be added after sealing. Reclaimed entries have
        // already supplied their actual ACK; the remaining exact records are
        // the receipt itself, not a temporary copied record/witness Vec.
        state
            .records
            .values()
            .map(|record| {
                if !record.delivered.load(Ordering::Acquire) {
                    return None;
                }
                let registration = record.registration.lock().unwrap();
                let received = record.received.lock().unwrap();
                let (id, witness) = registration.as_ref()?;
                let received = received.as_ref()?;
                (*id == received.id()
                    && scope.same_scope(&witness.scope_identity())
                    && witness.matches_joined(received))
                .then(|| witness.clone())
            })
            .collect::<Option<Arc<[TaskJoinWitness]>>>()
    }
}

mod census;
mod iteration;
pub use census::DriverRegistrationsClosed;
pub use iteration::RegistrationIter;
pub(in crate::controlled_datagram::metadata::users) use iteration::RegistrationSequence;

impl<O: DriverKind, B: Send + Sync + 'static> DriverOwner<O, B> {
    /// Independent library completion retains its original repeated body reads.
    /// Kernel endpoint consumers use owned_registration_completion instead.
    pub fn registration_completion(&self) -> Option<DriverRegistrationsClosed> {
        let scope = self.scope_identity();
        Some(DriverRegistrationsClosed::library(
            scope.clone(),
            self.completion_registrations(&scope)?,
        ))
    }
}
