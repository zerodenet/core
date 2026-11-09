//! Pure layout of this Scope's concrete witness/join/error Arc allocations.
use super::{ComponentError, Joined, JoinedState, TaskJoinWitness, TaskJoined};
use zero_traits::packet_metadata::{PacketMemoryError, PacketMetadataLayout};

impl TaskJoinWitness {
    /// Peak liability for the actual optional witness, consumed join and failed
    /// result allocations issued by `new`/`complete` in this same module.
    ///
    /// This computes storage only: it neither reserves credit nor observes an
    /// allocation's destruction. The actual receiver Book must prepay and keep
    /// the credit for its proven original/header lifetime, including registration
    /// Weak custody. Releasing/reusing a delivery cell alone proves none of that.
    /// Foreign source payloads, notification/queue allocations and arbitrary
    /// existing public raw Arc/Weak escape are outside these three layouts.
    pub(crate) fn task_failure_header_layout() -> Result<PacketMetadataLayout, PacketMemoryError> {
        PacketMetadataLayout::arc::<JoinedState>()?
            .checked_add(PacketMetadataLayout::arc::<Joined>()?)?
            .checked_add(PacketMetadataLayout::arc::<ComponentError>()?)
    }
}

impl TaskJoined {
    /// Borrow this same original result for the closed receiver delivery view.
    /// No Arc/Weak handle leaves its actual Book through this projection.
    pub(crate) fn error_ref(&self) -> Option<&ComponentError> {
        self.0.result.as_ref().err().map(std::sync::Arc::as_ref)
    }
}

impl TaskJoinWitness {
    /// Consume only exclusive, successful, actually released metadata originals.
    /// No surviving observer can see a completed witness become uncompleted.
    /// The caller must retain its prepaid liability and call outside Book locks.
    /// Failure returns both original handles for that same bounded retirement
    /// responsibility; success reports allocation destruction only, never IO,
    /// native closure, task exit, local failure acceptance or Host ACK.
    pub(crate) fn try_retire_success_headers(
        mut witness: Self,
        mut joined: TaskJoined,
    ) -> Result<(), (Self, TaskJoined)> {
        if joined.0.result.is_err() || !joined.registration_released() {
            return Err((witness, joined));
        }
        let matches = witness
            .0
            .joined
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|actual| actual.same_receipt(&joined));
        if !matches {
            return Err((witness, joined));
        }
        // get_mut atomically excludes every other strong/weak handle. Separate
        // count snapshots would race a concurrent Weak::upgrade/drop/downgrade.
        let Some(state) = std::sync::Arc::get_mut(&mut witness.0) else {
            return Err((witness, joined));
        };
        let original = state
            .joined
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("same unique witness already owns its actual join");
        // The input still strongly owns this same Joined allocation, so this
        // decrement cannot destroy its body or run any foreign destructor.
        drop(original);
        if std::sync::Arc::get_mut(&mut joined.0).is_none() {
            // This state is exclusive and unobservable. Restore the identical
            // receipt before returning either handle to any possible observer.
            *state
                .joined
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(joined.clone());
            return Err((witness, joined));
        }
        // Both actual headers are now exclusive, with no Weak upgrade path.
        // Real deallocation precedes the caller's eventual credit return.
        drop(witness);
        drop(joined);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
