use super::*;
impl MetadataPoolPermit {
    /// Native/users close remains independently observable. Only this owner's
    /// local reservation credit waits for every actual registration's Host ACK.
    pub(in crate::controlled_datagram::metadata_pool) fn release_registration_credit(
        &self,
    ) -> bool {
        let phase = self
            .0
            .protocol
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(cohort) = self.0.close_cohort.get() {
            if let Some(failure) = phase.as_ref().and_then(|phase| phase.resource_failure()) {
                cohort.receive_protocol(failure);
            }
            if !cohort.fault_received() {
                return false;
            }
        }
        if phase.as_ref().is_some_and(|phase| !phase.fault_received()) {
            return false;
        }
        if self.0.closed.get().is_none() {
            return false;
        }
        if self
            .0
            .registration_uncertain
            .load(std::sync::atomic::Ordering::Acquire)
            && !self
                .0
                .registration_scope
                .as_ref()
                .is_some_and(|scope| scope.is_drained())
        {
            return false;
        }
        let (caller, users) = {
            let Ok(native) = self.0.native() else {
                return false;
            };
            (self.0.caller.get().cloned(), native.confirmed.clone())
        };
        // Kernel receipts clone only the same sealed original record storage.
        // Iterate its real witnesses outside this owner's/native locks.
        let released = |witness: &TaskJoinWitness| {
            witness.try_joined().is_some_and(|joined| {
                witness.matches_joined(&joined) && joined.registration_released()
            })
        };
        if caller.as_ref().is_some_and(|caller| !released(caller))
            || users.as_ref().is_some_and(|users| {
                !users
                    .registrations()
                    .iter()
                    .chain(users.related_registrations())
                    .all(|witness| released(&witness))
            })
        {
            return false;
        }
        if let Some(owner) = self.0.owner.upgrade() {
            let removed = {
                let mut state = owner.state.lock().expect("metadata pool poisoned");
                if state
                    .slots
                    .get(&self.0.ordinal)
                    .is_some_and(|actual| Arc::ptr_eq(actual, &self.0))
                {
                    state.slots.remove(&self.0.ordinal)
                } else {
                    None
                }
            };
            let actually_removed = removed.is_some();
            drop(removed);
            if actually_removed {
                let phase = self
                    .0
                    .protocol
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                if let Some(phase) = phase {
                    if !self.0.root_bound {
                        phase.settle();
                    }
                }
                // Publish this exact local original's return outside all
                // owner locks. Root-bound public settlement is later published
                // only by actual global row removal, never this local ACK.
                self.0.closed.publish_registration_return();
            }
        }
        self.0
            .closed
            .observation(self.0.instance, self.0.ordinal)
            .released()
            .is_some()
    }
}
